package it.uniroma2.sabd.flink.query.query2;

import it.uniroma2.sabd.model.FlightEvent;
import it.uniroma2.sabd.flink.model.Query2Stats;
import it.uniroma2.sabd.flink.model.Query2Stats.DelayedFlight;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
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

    private ValueState<Query2Accumulator.State> accState;
    private ValueState<Long> windowStartState;
    private ValueState<Long> nextTimerState;

    @Override
    public void open(Configuration parameters) {
        accState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("global-q2-acc", Query2Accumulator.State.class));
        windowStartState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("global-q2-start", Long.class));
        nextTimerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("global-q2-timer", Long.class));
    }

    @Override
    public void processElement(FlightEvent event, Context ctx, Collector<Query2Stats> out) throws Exception {
        Query2Accumulator.State state = accState.value();

        if (state == null) {
            state = new Query2Accumulator.State();
            state.originAirportId = event.getOriginAirportId();
            windowStartState.update(event.getEventTime().toEpochSecond(ZoneOffset.UTC) * 1000L);
        }

        state.numFlights++;
        state.depDelaySum += event.getDepDelay();
        state.depDelayMax = Math.max(state.depDelayMax, event.getDepDelay());

        if (event.getDepDelay() > SEVERE_DELAY_THRESHOLD) {
            state.severeDelays++;
            state.delayedFlights.add(
                    new DelayedFlight(
                            event.getCarrier(),
                            event.getDestAirportId(), // FIX: Destinazione, non Origine
                            event.getDepDelay()));

            if (state.delayedFlights.size() > MAX_DELAYED_FLIGHTS) {
                state.delayedFlights.sort(
                        Comparator.comparingDouble(DelayedFlight::getDepDelay).reversed());
                state.delayedFlights = new ArrayList<>(
                        state.delayedFlights.subList(0, MAX_DELAYED_FLIGHTS));
            }
        }

        accState.update(state);

        Long nextTimer = nextTimerState.value();
        if (nextTimer == null) {
            long next = nextMonthlyTimer(ctx.timestamp());
            ctx.timerService().registerEventTimeTimer(next);
            nextTimerState.update(next);
        }
    }

    @Override
    public void onTimer(
            long timestamp,
            OnTimerContext ctx,
            Collector<Query2Stats> out) throws Exception {

        Query2Accumulator.State state = accState.value();
        Long windowStart = windowStartState.value();

        if (state != null && state.numFlights >= MIN_FLIGHTS && windowStart != null) {
            state.delayedFlights.sort(
                    Comparator.comparingDouble(DelayedFlight::getDepDelay).reversed());
            List<DelayedFlight> top20 = state.delayedFlights.size() > MAX_DELAYED_FLIGHTS
                    ? new ArrayList<>(state.delayedFlights.subList(0, MAX_DELAYED_FLIGHTS))
                    : new ArrayList<>(state.delayedFlights);

            out.collect(new Query2Stats(
                    Instant.ofEpochMilli(windowStart),
                    Instant.ofEpochMilli(timestamp), // FIX: Il timestamp di attivazione fa da "windowEnd"
                    state.originAirportId,           // FIX: Rimossa la virgola errata di sintassi "state.,"
                    state.numFlights,
                    state.severeDelays,
                    state.numFlights > 0 ? state.depDelaySum / state.numFlights : 0.0,
                    state.depDelayMax,
                    top20));
        }

        long next = nextMonthlyTimer(timestamp);
        ctx.timerService().registerEventTimeTimer(next);
        nextTimerState.update(next);
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
}