package it.uniroma2.sabd.flink.query.query2;

import it.uniroma2.sabd.model.FlightEvent;
import it.uniroma2.sabd.flink.model.Query2Stats.DelayedFlight;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.api.common.functions.AggregateFunction;

final class Query2Accumulator
        implements AggregateFunction<FlightEvent, Query2Accumulator.State, Query2AggregatedStats> {

    private static final double SEVERE_DELAY_THRESHOLD = 30.0;
    private static final int    MAX_DELAYED_FLIGHTS    = 20;

    @Override
    public State createAccumulator() {
        return new State();
    }

    @Override
    public State add(FlightEvent event, State state) {
        state.originAirportId = event.getOriginAirportId();
        state.numFlights++;
        state.depDelaySum += event.getDepDelay();
        state.depDelayMax  = Math.max(state.depDelayMax, event.getDepDelay());

        if (event.getDepDelay() > SEVERE_DELAY_THRESHOLD) {
            state.severeDelays++;
            state.delayedFlights.add(
                    new DelayedFlight(
                            event.getCarrier(),
                            event.getOriginAirportId(),
                            event.getDepDelay()));

            // Appena superiamo MAX, ordiniamo e tagliamo subito.
            // In memoria avremo sempre al massimo MAX+1 elementi.
            if (state.delayedFlights.size() > MAX_DELAYED_FLIGHTS) {
                state.delayedFlights.sort(
                        Comparator.comparingDouble(DelayedFlight::getDepDelay).reversed());
                state.delayedFlights = new ArrayList<>(
                        state.delayedFlights.subList(0, MAX_DELAYED_FLIGHTS));
            }
        }
        return state;
    }

    @Override
    public Query2AggregatedStats getResult(State state) {
        // La lista è già ordinata e tagliata a MAX — nessun sort extra necessario.
        return new Query2AggregatedStats(
                state.originAirportId,
                state.numFlights,
                state.severeDelays,
                state.depDelaySum,
                state.depDelayMax,
                new ArrayList<>(state.delayedFlights));
    }

    @Override
    public State merge(State left, State right) {
        left.numFlights    += right.numFlights;
        left.severeDelays  += right.severeDelays;
        left.depDelaySum   += right.depDelaySum;
        left.depDelayMax    = Math.max(left.depDelayMax, right.depDelayMax);
        left.delayedFlights.addAll(right.delayedFlights);

        // mantiene solo le prime 20(+1, cioè quel valore con cui fare il confronto).
        if (left.delayedFlights.size() > MAX_DELAYED_FLIGHTS) {
            left.delayedFlights.sort(
                    Comparator.comparingDouble(DelayedFlight::getDepDelay).reversed());
            left.delayedFlights = new ArrayList<>(
                    left.delayedFlights.subList(0, MAX_DELAYED_FLIGHTS));
        }
        return left;
    }

    static final class State implements Serializable {
        int    originAirportId = 0;
        long   numFlights      = 0;
        long   severeDelays    = 0;
        double depDelaySum     = 0.0;
        double depDelayMax     = Double.NEGATIVE_INFINITY;
        List<DelayedFlight> delayedFlights = new ArrayList<>();
    }
}