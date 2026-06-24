package it.uniroma2.sabd.flink.query.query3;

import it.uniroma2.sabd.model.Query3Stats;
import java.time.Instant;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

final class FinalizeQuery3Stats implements WindowFunction<Query3AggregatedStats, Query3Stats, Query3Key, TimeWindow> {

    @Override
    public void apply(
            Query3Key key,
            TimeWindow window,
            Iterable<Query3AggregatedStats> aggregatedStats,
            Collector<Query3Stats> out) {
        Query3AggregatedStats stats = aggregatedStats.iterator().next();

        out.collect(Query3Stats.fromDigest(
                Instant.ofEpochMilli(window.getStart()),
                Instant.ofEpochMilli(window.getEnd()),
                key.getAirline(),
                key.getDepartureHour(),
                stats.count,
                stats.min,
                stats.digest,
                stats.max));
    }

}
