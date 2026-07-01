package it.uniroma2.sabd.flink.model;

import java.time.Instant;
import static it.uniroma2.sabd.flink.utils.EventTimeUtils.CSV_FORMATTER;

public class Query1Stats extends AbstractQueryStats {

    private final String airline;
    private final long num_flights;
    private final long completed_flights;
    private final long cancelled_flights;
    private final long diverted_flights;
    private final double avg_dep_delay;
    private final double cancellation_rate;
    private final double late_departure_rate;

    public Query1Stats(
            Instant windowStart,
            Instant windowEnd,
            String airline,
            long num_flights,
            long completed_flights,
            long cancelled_flights,
            long diverted_flights,
            double avg_dep_delay,
            double cancellation_rate,
            double late_departure_rate,
            long processingStartTimeMs) {
        super(windowStart, windowEnd, processingStartTimeMs);
        this.airline = airline;
        this.num_flights = num_flights;
        this.completed_flights = completed_flights;
        this.cancelled_flights = cancelled_flights;
        this.diverted_flights = diverted_flights;
        this.avg_dep_delay = avg_dep_delay;
        this.cancellation_rate = cancellation_rate;
        this.late_departure_rate = late_departure_rate;
    }

    @Override
    public String toCSV() {
        return String.format("%s,%s,%s,%d,%d,%d,%d,%.2f,%.4f,%.4f",
                CSV_FORMATTER.format(windowStart),
                CSV_FORMATTER.format(windowEnd),
                airline,
                num_flights,
                completed_flights,
                cancelled_flights,
                diverted_flights,
                avg_dep_delay,
                cancellation_rate,
                late_departure_rate);
    }
}