package it.uniroma2.sabd.flink.query.query3;

import com.tdunning.math.stats.TDigest;
import it.uniroma2.sabd.flink.model.Query3GlobalStats;
import it.uniroma2.sabd.flink.model.Query3Key;
import it.uniroma2.sabd.flink.query.common.GlobalWindowProcessFunction;
import it.uniroma2.sabd.flink.utils.EventTimeUtils;
import it.uniroma2.sabd.model.FlightEvent;
import java.time.Instant;
import org.apache.flink.util.Collector;

/**
 * Finestra "dall'inizio del dataset" per Q3.
 * La struttura del timer mensile vive in GlobalWindowProcessFunction.
 * La logica di aggiornamento/merge dello stato è riusata da Query3Accumulator,
 * esattamente come GlobalQuery2ProcessFunction riusa Query2Accumulator —
 * zero duplicazione della logica del TDigest tra finestra tumbling e globale.
 */
final class GlobalTDigestProcessFunction
        extends GlobalWindowProcessFunction<Query3Key, Query3Accumulator.State, Query3GlobalStats> {

    private static final double TDIGEST_COMPRESSION = 100.0;

    private final Query3Accumulator accumulatorLogic = new Query3Accumulator();

    @Override
    protected String stateName() {
        return "global-q3";
    }

    @Override
    protected Class<Query3Accumulator.State> accumulatorClass() {
        return Query3Accumulator.State.class;
    }

    @Override
    protected Query3Accumulator.State createAccumulator(FlightEvent firstEvent) {
        return emptyAccumulator();
    }

    @Override
    protected Query3Accumulator.State emptyAccumulator() {
        Query3Accumulator.State state = new Query3Accumulator.State();
        state.min = Double.POSITIVE_INFINITY;
        state.max = Double.NEGATIVE_INFINITY;
        state.digest = TDigest.createDigest(TDIGEST_COMPRESSION);
        return state;
    }

    @Override
    protected void updateAccumulator(Query3Accumulator.State state, FlightEvent event) {
        accumulatorLogic.add(event, state);
    }

    @Override
    protected void mergeInto(Query3Accumulator.State target, Query3Accumulator.State source) {
        accumulatorLogic.merge(target, source);
    }

    @Override
    protected void emitIfValid(
            Query3Accumulator.State cumulative,
            long timestamp,
            Query3Key key,
            Collector<Query3GlobalStats> out) {

        if (cumulative.count == 0) {
            return;
        }

        cumulative.digest.compress();

        out.collect(new Query3GlobalStats(
                Instant.ofEpochMilli(EventTimeUtils.globalStart(timestamp)),
                Instant.ofEpochMilli(timestamp),
                key.getAirline(),
                key.getDepartureHour(),
                cumulative.count,
                cumulative.min,
                quantile(cumulative.digest, 0.25),
                quantile(cumulative.digest, 0.50),
                quantile(cumulative.digest, 0.75),
                quantile(cumulative.digest, 0.90),
                cumulative.max,
                cumulative.processingStartTimeMs));
    }

    private double quantile(TDigest digest, double q) {
        if (digest == null || digest.size() == 0) {
            return 0.0;
        }
        return digest.quantile(q);
    }
}