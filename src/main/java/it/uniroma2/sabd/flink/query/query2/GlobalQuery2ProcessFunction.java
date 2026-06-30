package it.uniroma2.sabd.flink.query.query2;

import it.uniroma2.sabd.flink.model.Query2GlobalStats;
import it.uniroma2.sabd.flink.model.Query2Stats;
import it.uniroma2.sabd.flink.model.Query2Stats.DelayedFlight;
import it.uniroma2.sabd.flink.query.common.GlobalWindowProcessFunction;
import it.uniroma2.sabd.flink.utils.EventTimeUtils;
import it.uniroma2.sabd.model.FlightEvent;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.util.Collector;

/**
 * Finestra "dall'inizio del dataset" per Q2.
 * Tutta la logica di stato mensile, timer e scarto dei late event
 * è in GlobalWindowProcessFunction. Qui resta solo la logica di dominio:
 * come si aggiorna l'accumulatore di Q2 e come si costruisce l'output.
 */
final class GlobalQuery2ProcessFunction
        extends GlobalWindowProcessFunction<Integer, Query2Accumulator.State, Query2Stats> {

    private static final int MIN_FLIGHTS = 30;

    // Riusa la logica di add()/merge() già scritta in Query2Accumulator —
    // niente duplicazione della logica di business tra finestra tumbling e globale.
    private final Query2Accumulator accumulatorLogic = new Query2Accumulator();

    @Override
    protected String stateName() {
        return "global-q2";
    }

    @Override
    protected Class<Query2Accumulator.State> accumulatorClass() {
        return Query2Accumulator.State.class;
    }

    @Override
    protected Query2Accumulator.State createAccumulator(FlightEvent firstEvent) {
        Query2Accumulator.State state = new Query2Accumulator.State();
        state.originAirportId = firstEvent.getOriginAirportId();
        return state;
    }

    @Override
    protected Query2Accumulator.State emptyAccumulator() {
        return new Query2Accumulator.State();
    }

    @Override
    protected void updateAccumulator(Query2Accumulator.State state, FlightEvent event) {
        accumulatorLogic.add(event, state);
    }

    @Override
    protected void mergeInto(Query2Accumulator.State target, Query2Accumulator.State source) {
        accumulatorLogic.merge(target, source);
    }

    @Override
    protected void emitIfValid(
            Query2Accumulator.State cumulative,
            long timestamp,
            Integer originAirportId,
            Collector<Query2Stats> out) {

        if (cumulative.numFlights < MIN_FLIGHTS) {
            return;
        }

        cumulative.delayedFlights.sort(
                Comparator.comparingDouble(DelayedFlight::getDepDelay).reversed());
        List<DelayedFlight> top20 = new ArrayList<>(cumulative.delayedFlights);

        out.collect(new Query2GlobalStats(
                Instant.ofEpochMilli(EventTimeUtils.globalStart(timestamp)),
                Instant.ofEpochMilli(timestamp),
                cumulative.originAirportId,
                cumulative.numFlights,
                cumulative.severeDelays,
                cumulative.numFlights > 0 ? cumulative.depDelaySum / cumulative.numFlights : 0.0,
                cumulative.depDelayMax,
                top20,
                cumulative.processingStartTimeMs));
    }
}