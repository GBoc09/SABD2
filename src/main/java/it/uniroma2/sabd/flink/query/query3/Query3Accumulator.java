package it.uniroma2.sabd.flink.query.query3;

import it.uniroma2.sabd.model.FlightEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

import com.tdunning.math.stats.TDigest;

final class Query3Accumulator implements AggregateFunction<FlightEvent, Query3Accumulator.State, Query3AggregatedStats> {
    /*
     * Questa classe definisce come aggregare gli eventi dentro una
     * finestra temporale per calcolare le statistiche richieste dalla prima query.
     */

    @Override
    // Inizializza lo stato parziale della finestra.
    public State createAccumulator() {
        State state = new State();
        state.count = 0;
        state.min = Double.POSITIVE_INFINITY;
        state.max = Double.NEGATIVE_INFINITY;
        state.digest = TDigest.createDigest(100.0);
        return state;
    }

    @Override
    public State add(FlightEvent event, State state){
        double depDelay = event.getDepDelay();

        state.count++;
        state.min = Math.min(state.min, depDelay);
        state.max = Math.max(state.max, depDelay);
        state.digest.add(depDelay);

        return state;
    }

    @Override
    public Query3AggregatedStats getResult(State state){
        state.digest.compress();

        double min = 0.0;
        double max = 0.0;
        if (state.count > 0) {
            min = state.min;
            max = state.max;
        }

        return new Query3AggregatedStats(
                state.count,
                min,
                max,
                state.digest);
    }

    @Override
    public State merge(State left, State right){
        left.count += right.count;
        left.min = Math.min(left.min, right.min);
        left.max = Math.max(left.max, right.max);
        left.digest.add(right.digest);
        return left;
    }

    static final class State {
        private long count;
        private double min;
        private double max;
        private TDigest digest;
    }
}
