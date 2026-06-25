package it.uniroma2.sabd.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;

/**
 * Risultato aggregato per un singolo aeroporto in una finestra.
 * Usato sia come output di FinalizeQuery2Stats / GlobalQuery2ProcessFunction
 * sia come input/output di RankingProcessFunction (che aggiunge il campo rank).
 */
public class Query2Stats implements Serializable {

    private Instant windowStart;
    private int rank;               // 0 prima del ranking, 1-10 dopo
    private int originAirportId;
    private long numFlights;
    private long severeDelays;
    private double depDelayMean;
    private double depDelayMax;
    private List<DelayedFlight> delayedFlights;

    public Query2Stats() {}

    public Query2Stats(
            Instant windowStart,
            int originAirportId,
            long numFlights,
            long severeDelays,
            double depDelayMean,
            double depDelayMax,
            List<DelayedFlight> delayedFlights) {
        this.windowStart    = windowStart;
        this.originAirportId = originAirportId;
        this.numFlights     = numFlights;
        this.severeDelays   = severeDelays;
        this.depDelayMean   = depDelayMean;
        this.depDelayMax    = depDelayMax;
        this.delayedFlights = delayedFlights;
        this.rank           = 0;
    }

    // ts, rank, origin_airport_id, num_flights, severe_delays,
    // dep_delay_mean, dep_delay_max, delayed_flights
    public String toCSV() {
        return String.format("%s,%d,%d,%d,%d,%.2f,%.2f,%s",
                windowStart,
                rank,
                originAirportId,
                numFlights,
                severeDelays,
                depDelayMean,
                depDelayMax,
                delayedFlights);
    }

    // Usato come chiave nel secondo stage di ranking
    public long getWindowStartEpoch() {
        return windowStart != null ? windowStart.toEpochMilli() : 0L;
    }

    public Instant getWindowStart()         { return windowStart; }
    public int getRank()                    { return rank; }
    public void setRank(int rank)           { this.rank = rank; }
    public int getOriginAirportId()         { return originAirportId; }
    public long getNumFlights()             { return numFlights; }
    public long getSevereDelays()           { return severeDelays; }
    public double getDepDelayMean()         { return depDelayMean; }
    public double getDepDelayMax()          { return depDelayMax; }
    public List<DelayedFlight> getDelayedFlights() { return delayedFlights; }

    /** Volo con ritardo significativo — inner class come in Q3AggregatedStats. */
    public static class DelayedFlight implements Serializable {
        public String carrier;
        public int originAirportId;   // era destAirportId
        public double depDelay;

        public DelayedFlight() {}

        public DelayedFlight(String carrier, int destAirportId, double depDelay) {
            this.carrier      = carrier;
            this.originAirportId = destAirportId;
            this.depDelay     = depDelay;
        }

        public double getDepDelay() { return depDelay; }

        @Override
        public String toString() {
            return String.format("(%s,%d,%.2f)", carrier, originAirportId, depDelay);
        }
    }
}