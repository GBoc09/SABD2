package it.uniroma2.sabd.flink.query.query2;

import it.uniroma2.sabd.model.FlightEvent;
import it.uniroma2.sabd.flink.model.Query2Stats;
import it.uniroma2.sabd.flink.model.Query2GlobalStats;
import it.uniroma2.sabd.flink.model.Query2Stats.DelayedFlight;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Finestra "dall'inizio del dataset" per Q2.
 * Stesso pattern di GlobalTDigestProcessFunction:
 * - KeyedProcessFunction (chiave = originAirportId)
 * - ValueState per lo stato accumulato
 * - Timer mensile su event time per emettere risultati periodici
 */
final class GlobalQuery2ProcessFunction
        extends KeyedProcessFunction<Integer, FlightEvent, Query2Stats> {

    private static final double SEVERE_DELAY_THRESHOLD = 30.0;
    private static final int    MAX_DELAYED_FLIGHTS    = 20;
    private static final int    MIN_FLIGHTS            = 30;

    private MapState<Long, Query2Accumulator.State> monthlyStatsState;
    private ValueState<Long> nextTimerState;

    @Override
    public void open(Configuration parameters) {
        monthlyStatsState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>(
                        "global-q2-monthly-stats",
                        Long.class,
                        Query2Accumulator.State.class));
        nextTimerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("global-q2-timer", Long.class));
    }

    @Override
    public void processElement(FlightEvent event, Context ctx, Collector<Query2Stats> out) throws Exception {
        long eventTimestamp = eventTimestamp(ctx, event);
        long currentWatermark = ctx.timerService().currentWatermark();

        // FIX (Punto 2): Scarta se late, coerente con le finestre 1h/6h.
        // Il WatermarkLateEventDetector ha già segnalato l'evento nei log se necessario,
        // qui lo droppiamo definitivamente per non inquinare il calcolo globale.
        if (currentWatermark != Long.MIN_VALUE && eventTimestamp <= currentWatermark) {
            return;
        }

        long monthStart = monthStart(eventTimestamp);
        Query2Accumulator.State state = monthlyStatsState.get(monthStart);
        if (state == null) {
            state = new Query2Accumulator.State();
            state.originAirportId = event.getOriginAirportId();
        }

        state.numFlights++;
        state.depDelaySum += event.getDepDelay();
        state.depDelayMax = Math.max(state.depDelayMax, event.getDepDelay());
        state.processingStartTimeMs =
                Math.max(state.processingStartTimeMs, event.getProcessingStartTimeMs());

        if (event.getDepDelay() > SEVERE_DELAY_THRESHOLD) {
            state.severeDelays++;
            state.delayedFlights.add(
                    new DelayedFlight(
                            event.getCarrier(),
                            event.getDestAirportId(),
                            event.getDepDelay()));

            if (state.delayedFlights.size() > MAX_DELAYED_FLIGHTS) {
                state.delayedFlights.sort(
                        Comparator.comparingDouble(DelayedFlight::getDepDelay).reversed());
                state.delayedFlights = new ArrayList<>(
                        state.delayedFlights.subList(0, MAX_DELAYED_FLIGHTS));
            }
        }

        monthlyStatsState.put(monthStart, state);

        Long nextTimer = nextTimerState.value();
        if (nextTimer == null) {
            long next = nextMonthlyTimer(eventTimestamp);
            ctx.timerService().registerEventTimeTimer(next);
            nextTimerState.update(next);
        }
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<Query2Stats> out) throws Exception {

        Query2Accumulator.State state = cumulativeStatsBefore(timestamp, ctx.getCurrentKey());

        if (state.numFlights >= MIN_FLIGHTS) {
            state.delayedFlights.sort(
                    Comparator.comparingDouble(DelayedFlight::getDepDelay).reversed());
            List<DelayedFlight> top20 = state.delayedFlights.size() > MAX_DELAYED_FLIGHTS
                    ? new ArrayList<>(state.delayedFlights.subList(0, MAX_DELAYED_FLIGHTS))
                    : new ArrayList<>(state.delayedFlights);

            // FIX (Punto 1): Emettiamo Query2GlobalStats invece di Query2Stats
            out.collect(new Query2GlobalStats(
                    Instant.ofEpochMilli(globalStart(timestamp)), // windowStart (Inizio Anno)
                    Instant.ofEpochMilli(timestamp),              // windowEnd (Timestamp dello Snapshot)
                    state.originAirportId,
                    state.numFlights,
                    state.severeDelays,
                    state.numFlights > 0 ? state.depDelaySum / state.numFlights : 0.0,
                    state.depDelayMax,
                    top20,
                    state.processingStartTimeMs));
        }

        long next = nextMonthlyTimer(timestamp);
        ctx.timerService().registerEventTimeTimer(next);
        nextTimerState.update(next);
    }

    private Query2Accumulator.State cumulativeStatsBefore(long snapshotTs, int originAirportId) throws Exception {
        Query2Accumulator.State cumulative = new Query2Accumulator.State();
        cumulative.originAirportId = originAirportId;

        for (Map.Entry<Long, Query2Accumulator.State> entry : monthlyStatsState.entries()) {
            if (entry.getKey() < snapshotTs) {
                mergeInto(cumulative, entry.getValue());
            }
        }

        return cumulative;
    }

    private void mergeInto(Query2Accumulator.State target, Query2Accumulator.State source) {
        target.numFlights += source.numFlights;
        target.severeDelays += source.severeDelays;
        target.depDelaySum += source.depDelaySum;
        target.depDelayMax = Math.max(target.depDelayMax, source.depDelayMax);
        target.processingStartTimeMs =
                Math.max(target.processingStartTimeMs, source.processingStartTimeMs);
        target.delayedFlights.addAll(source.delayedFlights);

        if (target.delayedFlights.size() > MAX_DELAYED_FLIGHTS) {
            target.delayedFlights.sort(
                    Comparator.comparingDouble(DelayedFlight::getDepDelay).reversed());
            target.delayedFlights = new ArrayList<>(
                    target.delayedFlights.subList(0, MAX_DELAYED_FLIGHTS));
        }
    }

    private long eventTimestamp(Context ctx, FlightEvent event) {
        Long timestamp = ctx.timestamp();
        if (timestamp != null) {
            return timestamp;
        }
        return event.getEventTime().toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    private long monthStart(long ts) {
        return Instant.ofEpochMilli(ts)
                .atZone(ZoneOffset.UTC)
                .withDayOfMonth(1)
                .truncatedTo(ChronoUnit.DAYS)
                .toInstant()
                .toEpochMilli();
    }

    private long nextMonthlyTimer(long ts) {
        return Instant.ofEpochMilli(ts)
                .atZone(ZoneOffset.UTC)
                .withDayOfMonth(1)
                .truncatedTo(ChronoUnit.DAYS)
                .plusMonths(1)
                .toInstant()
                .toEpochMilli();
    }

    private long globalStart(long timestamp) {
        return Instant.ofEpochMilli(timestamp)
                .atZone(ZoneOffset.UTC)
                .withDayOfYear(1)
                .truncatedTo(ChronoUnit.DAYS)
                .toInstant()
                .toEpochMilli();
    }
}