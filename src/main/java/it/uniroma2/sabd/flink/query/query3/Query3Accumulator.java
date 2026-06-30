package it.uniroma2.sabd.flink.query.query3;

import it.uniroma2.sabd.model.FlightEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

import com.tdunning.math.stats.TDigest;
import java.io.Serializable;

final class Query3Accumulator implements AggregateFunction<FlightEvent, Query3Accumulator.State, Query3AggregatedStats> {

    @Override
    public State createAccumulator() {
        State state = new State();
        state.count = 0;
        state.min = Double.POSITIVE_INFINITY;
        state.max = Double.NEGATIVE_INFINITY;
        state.digest = TDigest.createDigest(100.0);
        return state;
    }

    @Override
    public State add(FlightEvent event, State state) {
        double depDelay = event.getDepDelay();

        state.count++;
        state.min = Math.min(state.min, depDelay);
        state.max = Math.max(state.max, depDelay);
        state.digest.add(depDelay);
        state.processingStartTimeMs =
                Math.max(state.processingStartTimeMs, event.getProcessingStartTimeMs());

        return state;
    }

    @Override
    public Query3AggregatedStats getResult(State state) {
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
                state.digest,
                state.processingStartTimeMs);
    }

    @Override
    public State merge(State left, State right) {
        left.count += right.count;
        left.min = Math.min(left.min, right.min);
        left.max = Math.max(left.max, right.max);
        left.digest.add(right.digest);
        left.processingStartTimeMs =
                Math.max(left.processingStartTimeMs, right.processingStartTimeMs);
        return left;
    }

    /**
     * Stato mutabile dell'accumulatore.
     * Riusato anche da GlobalTDigestProcessFunction come accumulatore
     * della finestra globale, per evitare di duplicare add()/merge().
     */
    static final class State implements Serializable {
        long count;
        double min;
        double max;
        TDigest digest;
        long processingStartTimeMs;
    }
}