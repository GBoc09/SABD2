package it.uniroma2.sabd.flink.model;

import it.uniroma2.sabd.flink.io.sink.CsvValues;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Sottoclasse specifica per la finestra Globale di Query 2.
 * Mantiene windowEnd come timestamp dello snapshot per il ranking,
 * ma stampa windowStart come ts CSV, come richiesto dallo schema.
 */
public class Query2GlobalStats extends Query2Stats {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.of("UTC"));

    public Query2GlobalStats(
            Instant globalStart,
            Instant snapshotTime,
            int originAirportId,
            long numFlights,
            long severeDelays,
            double depDelayMean,
            double depDelayMax,
            List<DelayedFlight> delayedFlights,
            long processingStartTimeMs) {

        super(globalStart, snapshotTime, originAirportId,
                numFlights, severeDelays, depDelayMean, depDelayMax, delayedFlights, processingStartTimeMs);
    }

    @Override
    public String toCSV() {
        String delayedStr = "[" + getDelayedFlights().stream()
                .map(DelayedFlight::toString)
                .collect(Collectors.joining(", ")) + "]";

        return String.format(Locale.US, "%s,%d,%d,%d,%d,%.2f,%.2f,%s",
                FORMATTER.format(getWindowEnd()),
                getRank(),
                getOriginAirportId(),
                getNumFlights(),
                getSevereDelays(),
                getDepDelayMean(),
                getDepDelayMax(),
                CsvValues.text(delayedStr));
    }
}
