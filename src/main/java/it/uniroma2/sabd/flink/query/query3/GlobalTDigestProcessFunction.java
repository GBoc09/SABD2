package it.uniroma2.sabd.flink.query.query3;

import com.tdunning.math.stats.TDigest;
import it.uniroma2.sabd.flink.model.Query3GlobalStats;
import it.uniroma2.sabd.flink.model.Query3Key;
import it.uniroma2.sabd.model.FlightEvent;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

final class GlobalTDigestProcessFunction extends KeyedProcessFunction<Query3Key, FlightEvent, Query3GlobalStats> {
    private transient MapState<Long, Query3AggregatedStats> monthlyStatsState;
    private transient ValueState<Long> nextTimerState;

    @Override
    public void open(Configuration parameters) {
        monthlyStatsState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>(
                        "query3-global-monthly-stats",
                        Long.class,
                        Query3AggregatedStats.class));

        nextTimerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("query3-global-next-timer", Long.class));
    }

    @Override
    public void processElement(
            FlightEvent event,
            Context ctx,
            Collector<Query3GlobalStats> out) throws Exception {
        long eventTimestamp = eventTimestamp(ctx, event);
        registerFirstMonthlyTimer(ctx, eventTimestamp);

        double depDelay = event.getDepDelay();
        long monthStart = monthStart(eventTimestamp);
        Query3AggregatedStats currentStats = monthlyStatsState.get(monthStart);

        TDigest digest = TDigest.createDigest(100.0);
        long count = 1;
        double min = depDelay;
        double max = depDelay;
        long processingStartTimeMs = event.getProcessingStartTimeMs();

        if (currentStats != null) {
            digest = currentStats.digest;
            count = currentStats.count + 1;
            min = Math.min(currentStats.min, depDelay);
            max = Math.max(currentStats.max, depDelay);
            processingStartTimeMs =
                    Math.max(currentStats.processingStartTimeMs, processingStartTimeMs);
        }

        digest.add(depDelay);
        monthlyStatsState.put(
                monthStart,
                new Query3AggregatedStats(count, min, max, digest, processingStartTimeMs));
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<Query3GlobalStats> out) throws Exception {
        Query3AggregatedStats stats = cumulativeStatsBefore(timestamp);

        if (stats.count > 0) {
            stats.digest.compress();

            Query3Key key = ctx.getCurrentKey();
            out.collect(new Query3GlobalStats(
                    Instant.ofEpochMilli(globalStart(timestamp)),
                    Instant.ofEpochMilli(timestamp),
                    key.getAirline(),
                    key.getDepartureHour(),
                    stats.count,
                    stats.min,
                    quantile(stats.digest, 0.25),
                    quantile(stats.digest, 0.50),
                    quantile(stats.digest, 0.75),
                    quantile(stats.digest, 0.90),
                    stats.max,
                    stats.processingStartTimeMs));
        }

        long nextTimer = nextMonthlyTimer(timestamp);
        ctx.timerService().registerEventTimeTimer(nextTimer);
        nextTimerState.update(nextTimer);
    }

    private void registerFirstMonthlyTimer(Context ctx, long eventTimestamp) throws Exception {
        if (nextTimerState.value() == null) {
            long nextTimer = nextMonthlyTimer(eventTimestamp);
            ctx.timerService().registerEventTimeTimer(nextTimer);
            nextTimerState.update(nextTimer);
        }
    }

    private Query3AggregatedStats cumulativeStatsBefore(long snapshotTs) throws Exception {
        long count = 0L;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        long processingStartTimeMs = 0L;
        TDigest digest = TDigest.createDigest(100.0);

        for (Map.Entry<Long, Query3AggregatedStats> entry : monthlyStatsState.entries()) {
            if (entry.getKey() < snapshotTs) {
                Query3AggregatedStats stats = entry.getValue();
                count += stats.count;
                min = Math.min(min, stats.min);
                max = Math.max(max, stats.max);
                processingStartTimeMs =
                        Math.max(processingStartTimeMs, stats.processingStartTimeMs);
                digest.add(stats.digest);
            }
        }

        if (count == 0L) {
            return new Query3AggregatedStats(0L, 0.0, 0.0, digest, 0L);
        }

        return new Query3AggregatedStats(count, min, max, digest, processingStartTimeMs);
    }

    private long eventTimestamp(Context ctx, FlightEvent event) {
        Long timestamp = ctx.timestamp();
        if (timestamp != null) {
            return timestamp;
        }
        return event.getEventTime().toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    private long globalStart(long timestamp) {
        return Instant.ofEpochMilli(timestamp)
                .atZone(ZoneOffset.UTC)
                .withDayOfYear(1)
                .truncatedTo(ChronoUnit.DAYS)
                .toInstant()
                .toEpochMilli();
    }

    private long monthStart(long timestamp) {
        return Instant.ofEpochMilli(timestamp)
                .atZone(ZoneOffset.UTC)
                .withDayOfMonth(1)
                .truncatedTo(ChronoUnit.DAYS)
                .toInstant()
                .toEpochMilli();
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
