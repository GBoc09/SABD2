package it.uniroma2.sabd.flink.model;

import it.uniroma2.sabd.flink.io.sink.CsvValues;
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
public class Query2Stats extends AbstractQueryStats {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.of("UTC"));

    private int rank; // 0 prima del ranking, 1-10 dopo
    private final int originAirportId;
    private final long numFlights;
    private final long severeDelays;
    private final double depDelayMean;
    private final double depDelayMax;
    private final List<DelayedFlight> delayedFlights;

    public Query2Stats(
            Instant windowStart,
            Instant windowEnd,
            int originAirportId,
            long numFlights,
            long severeDelays,
            double depDelayMean,
            double depDelayMax,
            List<DelayedFlight> delayedFlights,
            long processingStartTimeMs) {
        super(windowStart, windowEnd, processingStartTimeMs);
        this.originAirportId = originAirportId;
        this.numFlights = numFlights;
        this.severeDelays = severeDelays;
        this.depDelayMean = depDelayMean;
        this.depDelayMax = depDelayMax;
        this.delayedFlights = delayedFlights;
        this.rank = 0;
    }

    public int getRank() { return rank; }
    public void setRank(int rank) { this.rank = rank; }
    public int getOriginAirportId() { return originAirportId; }
    public long getNumFlights() { return numFlights; }
    public long getSevereDelays() { return severeDelays; }
    public double getDepDelayMean() { return depDelayMean; }
    public double getDepDelayMax() { return depDelayMax; }
    public List<DelayedFlight> getDelayedFlights() { return delayedFlights; }

    @Override
    public String toCSV() {
        String delayedStr = "[" + delayedFlights.stream()
                .map(DelayedFlight::toString)
                .collect(Collectors.joining(", ")) + "]";

        return String.format(Locale.US, "%s,%d,%d,%d,%d,%.2f,%.2f,%s",
                FORMATTER.format(windowStart), rank, originAirportId,
                numFlights, severeDelays, depDelayMean, depDelayMax, CsvValues.text(delayedStr));
    }

    /** Volo con ritardo significativo, riportato nella lista delayed_flights dell'output. */
    public static class DelayedFlight implements Serializable {
        public String carrier;
        public int destAirportId; // aeroporto di destinazione, come da specifica
        public double depDelay;

        public DelayedFlight() {}

        public DelayedFlight(String carrier, int destAirportId, double depDelay) {
            this.carrier = carrier;
            this.destAirportId = destAirportId;
            this.depDelay = depDelay;
        }

        public double getDepDelay() { return depDelay; }

        @Override
        public String toString() {
            return String.format(Locale.US, "(%s,%d,%.2f)", carrier, destAirportId, depDelay);
        }
    }
}