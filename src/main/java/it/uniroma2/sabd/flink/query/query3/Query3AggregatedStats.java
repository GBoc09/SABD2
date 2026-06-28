package it.uniroma2.sabd.flink.query.query3;

import com.tdunning.math.stats.TDigest;

final class Query3AggregatedStats {
    final long count;
    final double min;
    final double max;
    final TDigest digest;
    final long processingStartTimeMs;

    /*
            • il numero di voli considerati;
            • il minimo di DEP DELAY;
            • il 25-esimo percentile;
            • il 50-esimo percentile, o mediana;
            • il 75-esimo percentile;
            • il 90-esimo percentile;
            • il massimo di DEP DELAY.
     */
    Query3AggregatedStats(
            long count,
            double min,
            double max,
            TDigest digest,
            long processingStartTimeMs) {
        this.count = count;
        this.min = min;
        this.max = max;
        this.digest = digest;
        this.processingStartTimeMs = processingStartTimeMs;
    }
}
