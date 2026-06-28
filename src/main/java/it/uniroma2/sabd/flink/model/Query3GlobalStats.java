package it.uniroma2.sabd.flink.model;

import java.time.Instant;
public class Query3GlobalStats extends Query3Stats {

    /*
     * Riusa i campi comuni di Query3Stats:
     * - windowStart rappresenta l'inizio dell'aggregazione globale;
     * - windowEnd rappresenta il timestamp dello snapshot emesso dal timer mensile.
     */
    public Query3GlobalStats(
            Instant globalStart,
            Instant snapshotTime,
            String airline,
            int departureHour,
            long num_flights,
            double min_dep_delay,
            double p25_dep_delay,
            double p50_dep_delay,
            double p75_dep_delay,
            double p90_dep_delay,
            double max_dep_delay,
            long processingStartTimeMs) {
        super(
                globalStart,
                snapshotTime,
                airline,
                departureHour,
                num_flights,
                min_dep_delay,
                p25_dep_delay,
                p50_dep_delay,
                p75_dep_delay,
                p90_dep_delay,
                max_dep_delay,
                processingStartTimeMs);
    }

    @Override
    public String toCSV() {
        return new Query3Stats(
                windowEnd,
                windowEnd,
                airline,
                departureHour,
                num_flights,
                min_dep_delay,
                p25_dep_delay,
                p50_dep_delay,
                p75_dep_delay,
                p90_dep_delay,
                max_dep_delay,
                processingStartTimeMs).toCSV();
    }
}
