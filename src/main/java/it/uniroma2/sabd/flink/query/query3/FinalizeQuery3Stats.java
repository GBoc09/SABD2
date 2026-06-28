package it.uniroma2.sabd.flink.query.query3;

import com.tdunning.math.stats.TDigest;
import it.uniroma2.sabd.flink.model.Query3Key;
import it.uniroma2.sabd.flink.model.Query3Stats;
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

        out.collect(new Query3Stats(
                Instant.ofEpochMilli(window.getStart()),
                Instant.ofEpochMilli(window.getEnd()),
                key.getAirline(),
                key.getDepartureHour(),
                stats.count,
                stats.min,
                quantile(stats.digest, 0.25),
                quantile(stats.digest, 0.50),
                quantile(stats.digest, 0.75),
                quantile(stats.digest, 0.90),
                stats.max,
                stats.processingStartTimeMs));
    }

    private double quantile(TDigest digest, double q) {
        if (digest == null || digest.size() == 0) {
            return 0.0;
        }
        return digest.quantile(q);
    }

}
