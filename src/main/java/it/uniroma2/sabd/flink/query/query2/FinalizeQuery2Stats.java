package it.uniroma2.sabd.flink.query.query2;

import it.uniroma2.sabd.flink.model.Query2Stats;
import java.time.Instant;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Riceve il risultato dell'Accumulator e produce Query2Stats con i metadati
 * della finestra. Stesso pattern di FinalizeQuery1Stats / FinalizeQuery3Stats.
 * Il campo rank viene lasciato a 0 — sarà valorizzato da RankingProcessFunction.
 */
final class FinalizeQuery2Stats
        implements WindowFunction<Query2AggregatedStats, Query2Stats, Integer, TimeWindow> {

    private static final int MIN_FLIGHTS = 30;

    @Override
    public void apply(
            Integer airportId,
            TimeWindow window,
            Iterable<Query2AggregatedStats> input,
            Collector<Query2Stats> out) {

        Query2AggregatedStats stats = input.iterator().next();

        if (stats.numFlights < MIN_FLIGHTS) return;

        out.collect(new Query2Stats(
                Instant.ofEpochMilli(window.getStart()),
                Instant.ofEpochMilli(window.getEnd()), // FIX: Inserito windowEnd qui!
                stats.originAirportId,
                stats.numFlights,
                stats.severeDelays,
                stats.depDelayMean(),
                stats.depDelayMax,
                stats.delayedFlights,
                stats.processingStartTimeMs));
    }
}
