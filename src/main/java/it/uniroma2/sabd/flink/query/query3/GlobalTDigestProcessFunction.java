package it.uniroma2.sabd.flink.query.query3;

import com.tdunning.math.stats.TDigest;
import it.uniroma2.sabd.flink.model.Query3GlobalStats;
import it.uniroma2.sabd.flink.model.Query3Key;
import it.uniroma2.sabd.model.FlightEvent;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

final class GlobalTDigestProcessFunction extends KeyedProcessFunction<Query3Key, FlightEvent, Query3GlobalStats> {
    private transient ValueState<Query3AggregatedStats> statsState;
    private transient ValueState<Long> windowStartState;
    private transient ValueState<Long> nextTimerState;

    @Override
    public void open(Configuration parameters) {
        statsState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("query3-global-stats", Query3AggregatedStats.class));

        windowStartState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("query3-global-window-start", Long.class));

        nextTimerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("query3-global-next-timer", Long.class));
    }

    @Override
    public void processElement(
            FlightEvent event,
            Context ctx,
            Collector<Query3GlobalStats> out) throws Exception {
        long eventTimestamp = eventTimestamp(ctx, event);
        initializeWindowStart(eventTimestamp);
        registerFirstMonthlyTimer(ctx, eventTimestamp);

        double depDelay = event.getDepDelay();
        Query3AggregatedStats currentStats = statsState.value();

        TDigest digest = TDigest.createDigest(100.0);
        long count = 1;
        double min = depDelay;
        double max = depDelay;

        if (currentStats != null) {
            digest = currentStats.digest;
            count = currentStats.count + 1;
            min = Math.min(currentStats.min, depDelay);
            max = Math.max(currentStats.max, depDelay);
        }

        digest.add(depDelay);
        statsState.update(new Query3AggregatedStats(count, min, max, digest));
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<Query3GlobalStats> out) throws Exception {
        Query3AggregatedStats stats = statsState.value();
        Long windowStart = windowStartState.value();

        if (stats != null && stats.count > 0 && windowStart != null) {
            stats.digest.compress();

            Query3Key key = ctx.getCurrentKey();
            out.collect(new Query3GlobalStats(
                    Instant.ofEpochMilli(windowStart),
                    Instant.ofEpochMilli(timestamp),
                    key.getAirline(),
                    key.getDepartureHour(),
                    stats.count,
                    stats.min,
                    quantile(stats.digest, 0.25),
                    quantile(stats.digest, 0.50),
                    quantile(stats.digest, 0.75),
                    quantile(stats.digest, 0.90),
                    stats.max));

            statsState.update(new Query3AggregatedStats(
                    stats.count,
                    stats.min,
                    stats.max,
                    stats.digest));
        }

        long nextTimer = nextMonthlyTimer(timestamp);
        ctx.timerService().registerEventTimeTimer(nextTimer);
        nextTimerState.update(nextTimer);
    }

    private void initializeWindowStart(long eventTimestamp) throws Exception {
        if (windowStartState.value() == null) {
            windowStartState.update(eventTimestamp);
        }
    }

    private void registerFirstMonthlyTimer(Context ctx, long eventTimestamp) throws Exception {
        if (nextTimerState.value() == null) {
            long nextTimer = nextMonthlyTimer(eventTimestamp);
            ctx.timerService().registerEventTimeTimer(nextTimer);
            nextTimerState.update(nextTimer);
        }
    }

    private long eventTimestamp(Context ctx, FlightEvent event) {
        Long timestamp = ctx.timestamp();
        if (timestamp != null) {
            return timestamp;
        }
        return event.getEventTime().toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    private long nextMonthlyTimer(long timestamp) {
        return Instant.ofEpochMilli(timestamp)
                .atZone(ZoneOffset.UTC)
                .withDayOfMonth(1)
                .truncatedTo(ChronoUnit.DAYS)
                .plusMonths(1)
                .toInstant()
                .toEpochMilli();
    }

    private double quantile(TDigest digest, double q) {
        if (digest == null || digest.size() == 0) {
            return 0.0;
        }
        return digest.quantile(q);
    }
}
