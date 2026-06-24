package it.uniroma2.sabd.model;

import java.time.Instant;
import java.util.Locale;

public class Query3Stats {

    /*
     * Modello base per Q3 sulle finestre tumbling.
     */
    protected Instant windowStart;
    protected Instant windowEnd;
    protected String airline;
    protected int departureHour;
    protected long num_flights;
    protected double min_dep_delay;
    protected double p25_dep_delay;
    protected double p50_dep_delay;
    protected double p75_dep_delay;
    protected double p90_dep_delay;
    protected double max_dep_delay;

    public Query3Stats(
            Instant windowStart,
            Instant windowEnd,
            String airline,
            int departureHour,
            long num_flights,
            double min_dep_delay,
            double p25_dep_delay,
            double p50_dep_delay,
            double p75_dep_delay,
            double p90_dep_delay,
            double max_dep_delay) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.airline = airline;
        this.departureHour = departureHour;
        this.num_flights = num_flights;
        this.min_dep_delay = min_dep_delay;
        this.p25_dep_delay = p25_dep_delay;
        this.p50_dep_delay = p50_dep_delay;
        this.p75_dep_delay = p75_dep_delay;
        this.p90_dep_delay = p90_dep_delay;
        this.max_dep_delay = max_dep_delay;
    }

    // final output format for Query3 results
    public String toCSV() {
        return String.format(Locale.ROOT, "%s,%s,%d,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f",
                windowStart.toString(),
                airline,
                departureHour,
                num_flights,
                min_dep_delay,
                p25_dep_delay,
                p50_dep_delay,
                p75_dep_delay,
                p90_dep_delay,
                max_dep_delay);
    }
}
