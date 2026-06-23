package it.uniroma2.sabd.flink.query.query1;

import it.uniroma2.sabd.model.Query1Stats;
import java.time.Instant;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

final class FinalizeQuery1Stats implements WindowFunction<Query1AggregatedStats, Query1Stats, String, TimeWindow> {

    @Override
    public void apply(
            String airline,
            TimeWindow window,
            Iterable<Query1AggregatedStats> aggregatedStats,
            Collector<Query1Stats> out) {
        Query1AggregatedStats stats = aggregatedStats.iterator().next();

        out.collect(new Query1Stats(
                Instant.ofEpochMilli(window.getStart()),
                Instant.ofEpochMilli(window.getEnd()),
                airline,
                stats.totalFlights,
                stats.completedFlights,
                stats.cancelledFlights,
                stats.divertedFlights,
                stats.avgDepDelay,
                stats.cancellationRate,
                stats.lateDepartureRate));
    }
}
