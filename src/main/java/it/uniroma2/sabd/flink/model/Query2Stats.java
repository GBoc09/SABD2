package it.uniroma2.sabd.flink.model;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Risultato aggregato per un singolo aeroporto in una finestra.
 * Usato sia come output di FinalizeQuery2Stats / GlobalQuery2ProcessFunction
 * sia come input/output di RankingProcessFunction (che aggiunge il campo rank).
 */
public class Query2Stats implements Serializable {

    // Aggiunto formatter per combaciare ESATTAMENTE col PDF (no T, no Z)
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.of("UTC"));

    private Instant windowStart;
    private Instant windowEnd;      // Aggiunto per gestire correttamente il timer in RankingProcessFunction
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
            Instant windowEnd,
            int originAirportId,
            long numFlights,
            long severeDelays,
            double depDelayMean,
            double depDelayMax,
            List<DelayedFlight> delayedFlights) {
        this.windowStart    = windowStart;
        this.windowEnd      = windowEnd;
        this.originAirportId = originAirportId;
        this.numFlights     = numFlights;
        this.severeDelays   = severeDelays;
        this.depDelayMean   = depDelayMean;
        this.depDelayMax    = depDelayMax;
        this.delayedFlights = delayedFlights;
        this.rank           = 0;
    }

    // ts per la chiave di raggruppamento
    public long getWindowStartEpoch() {
        return windowStart != null ? windowStart.toEpochMilli() : 0L;
    }

    // Per gestire il timer event-time in base alla fine della finestra
    public long getWindowEndEpoch() {
        return windowEnd != null ? windowEnd.toEpochMilli() : 0L;
    }

    public Instant getWindowStart()         { return windowStart; }
    public Instant getWindowEnd()           { return windowEnd; }
    public int getRank()                    { return rank; }
    public void setRank(int rank)           { this.rank = rank; }
    public int getOriginAirportId()         { return originAirportId; }
    public long getNumFlights()             { return numFlights; }
    public long getSevereDelays()           { return severeDelays; }
    public double getDepDelayMean()         { return depDelayMean; }
    public double getDepDelayMax()          { return depDelayMax; }
    public List<DelayedFlight> getDelayedFlights() { return delayedFlights; }

    // Generatore CSV per il Sink (Aggiunto per risolvere il formato data)
    public String toCSV() {
        String delayedStr = "[" + delayedFlights.stream()
                .map(DelayedFlight::toString)
                .collect(Collectors.joining(", ")) + "]";

        return String.format(Locale.US, "%s, %d, %d, %d, %d, %.2f, %.2f, %s",
                FORMATTER.format(windowStart), rank, originAirportId,
                numFlights, severeDelays, depDelayMean, depDelayMax, delayedStr);
    }

    /** Volo con ritardo significativo — inner class come in Q3AggregatedStats. */
    public static class DelayedFlight implements Serializable {
        public String carrier;
        public int destAirportId;   // Corretto: aeroporto di destinazione come da PDF
        public double depDelay;

        public DelayedFlight() {}

        public DelayedFlight(String carrier, int destAirportId, double depDelay) {
            this.carrier      = carrier;
            this.destAirportId = destAirportId;
            this.depDelay     = depDelay;
        }

        public double getDepDelay() { return depDelay; }

        @Override
        public String toString() {
            return String.format(Locale.US, "(%s,%d,%.2f)", carrier, destAirportId, depDelay);
        }
    }
}