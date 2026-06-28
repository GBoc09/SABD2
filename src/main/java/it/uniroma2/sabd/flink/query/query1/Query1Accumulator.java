package it.uniroma2.sabd.flink.query.query1;

import it.uniroma2.sabd.model.FlightEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

final class Query1Accumulator implements AggregateFunction<FlightEvent, Query1Accumulator.State, Query1AggregatedStats> {
    /*
     * Questa classe definisce come aggregare gli eventi dentro una
     * finestra temporale per calcolare le statistiche richieste dalla prima query.
     */

    @Override
    // Inizializza lo stato parziale della finestra.
    public State createAccumulator() {
        return new State();
    }

    @Override
    // Aggiorna lo stato con i dati del singolo volo ricevuto.
    public State add(FlightEvent event, State state) {
        boolean cancelled = event.getCancelled() > 0.0;
        boolean diverted = event.getDiverted() > 0.0;

        state.totalFlights++;
        state.processingStartTimeMs =
                Math.max(state.processingStartTimeMs, event.getProcessingStartTimeMs());

        if (cancelled) {
            state.cancelledFlights++;
        }

        if (diverted) {
            state.divertedFlights++;
        }

        if (!cancelled && !diverted) {
            state.completedFlights++;
        }

        if (!cancelled) {
            state.nonCancelledFlights++;
            state.depDelaySum += event.getDepDelay();

            if (event.getDepDelay() > 15.0) {
                state.lateDepartureFlights++;
            }
        }

        return state;
    }

    @Override
    // Converte lo stato accumulato nelle metriche aggregate della finestra.
    public Query1AggregatedStats getResult(State state) {
        double avgDepDelay = 0.0;
        if (state.nonCancelledFlights > 0) {
            avgDepDelay = state.depDelaySum / state.nonCancelledFlights;
        }

        double cancellationRate = 0.0;
        if (state.totalFlights > 0) {
            cancellationRate = (double) state.cancelledFlights / state.totalFlights * 100.0;
        }

        double lateDepartureRate = 0.0;
        if (state.nonCancelledFlights > 0) {
            lateDepartureRate = (double) state.lateDepartureFlights / state.nonCancelledFlights * 100.0;
        }

        return new Query1AggregatedStats(
                state.totalFlights,
                state.completedFlights,
                state.cancelledFlights,
                state.divertedFlights,
                avgDepDelay,
                cancellationRate,
                lateDepartureRate,
                state.processingStartTimeMs);
    }

    @Override
    // Unisce due stati parziali prodotti dalla stessa finestra.
    public State merge(State left, State right) {
        left.totalFlights += right.totalFlights;
        left.completedFlights += right.completedFlights;
        left.cancelledFlights += right.cancelledFlights;
        left.divertedFlights += right.divertedFlights;
        left.nonCancelledFlights += right.nonCancelledFlights;
        left.depDelaySum += right.depDelaySum;
        left.lateDepartureFlights += right.lateDepartureFlights;
        left.processingStartTimeMs =
                Math.max(left.processingStartTimeMs, right.processingStartTimeMs);
        return left;
    }

    static final class State {
        private long totalFlights;
        private long completedFlights;
        private long cancelledFlights;
        private long divertedFlights;
        private long nonCancelledFlights;
        private double depDelaySum;
        private long lateDepartureFlights;
        private long processingStartTimeMs;
    }
}
