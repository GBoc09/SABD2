package it.uniroma2.sabd.model;

import java.time.Instant;

public class Query1Stats{
    Instant windowStart;
    Instant windowEnd;
    String airline;
    long num_flights;
    long completed_flights;
    long cancelled_flights;
    long diverted_flights;
    double avg_dep_delay;
    double cancellation_rate;
    double late_departure_rate;

    public Query1Stats(Instant windowStart, Instant windowEnd, String airline, long num_flights, long completed_flights, long cancelled_flights, long diverted_flights, double avg_dep_delay, double cancellation_rate, double late_departure_rate) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.airline = airline;
        this.num_flights = num_flights;
        this.completed_flights = completed_flights;
        this.cancelled_flights = cancelled_flights;
        this.diverted_flights = diverted_flights;
        this.avg_dep_delay = avg_dep_delay;
        this.cancellation_rate = cancellation_rate;
        this.late_departure_rate = late_departure_rate;
    }

    // final output format for Query1 results
    public String toCSV() {
        return String.format("%s,%s,%s,%d,%d,%d,%d,%.2f,%.4f,%.4f",
                windowStart.toString(),
                windowEnd.toString(),
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
