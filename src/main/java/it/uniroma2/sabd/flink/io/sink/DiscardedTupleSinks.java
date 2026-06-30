package it.uniroma2.sabd.flink.io.sink;

import it.uniroma2.sabd.model.FlightEvent;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import org.apache.flink.streaming.api.datastream.DataStream;

public final class DiscardedTupleSinks {

    public static final String HEADER =
            "query,window,event_time,event_timestamp_ms,window_start,window_end,"
            + "carrier,origin_airport_id,dest_airport_id,crs_dep_time,dep_delay,"
            + "cancelled,diverted,produced_at";

    private DiscardedTupleSinks() {
    }

    public static void writeFixedWindow(
            DataStream<FlightEvent> discardedEvents,
            String watermark,
            String query,
            String window,
            Duration windowSize) {

        discardedEvents
                .map(event -> toCsv(query, window, event, fixedWindow(event, windowSize)))
                .sinkTo(QuerySinks.discardedTuplesCsv(watermark, query, window))
                .name("Discarded Tuples Sink " + watermark + " " + query + " " + window);
    }

    public static void writeMonthlyWindow(
            DataStream<FlightEvent> discardedEvents,
            String watermark,
            String query,
            String window) {

        discardedEvents
                .map(event -> toCsv(query, window, event, monthlyWindow(event)))
                .sinkTo(QuerySinks.discardedTuplesCsv(watermark, query, window))
                .name("Discarded Tuples Sink " + watermark + " " + query + " " + window);
    }

    private static String toCsv(
            String query,
            String window,
            FlightEvent event,
            WindowBounds bounds) {

        long eventTimestamp = eventTimestamp(event);

        return String.join(",",
                CsvValues.text(query),
                CsvValues.text(window),
                CsvValues.text(event.getEventTime() == null ? "" : event.getEventTime().toString()),
                Long.toString(eventTimestamp),
                CsvValues.text(Instant.ofEpochMilli(bounds.start).toString()),
                CsvValues.text(Instant.ofEpochMilli(bounds.end).toString()),
                CsvValues.text(event.getCarrier()),
                Integer.toString(event.getOriginAirportId()),
                Integer.toString(event.getDestAirportId()),
                Integer.toString(event.getCrsDepTime()),
                CsvValues.decimal(event.getDepDelay()),
                CsvValues.decimal(event.getCancelled()),
                CsvValues.decimal(event.getDiverted()),
                Long.toString(event.getProducedAt()));
    }

    private static WindowBounds fixedWindow(FlightEvent event, Duration windowSize) {
        long timestamp = eventTimestamp(event);
        long sizeMs = windowSize.toMillis();
        long start = timestamp - Math.floorMod(timestamp, sizeMs);
        return new WindowBounds(start, start + sizeMs);
    }

    private static WindowBounds monthlyWindow(FlightEvent event) {
        long start = Instant.ofEpochMilli(eventTimestamp(event))
                .atZone(ZoneOffset.UTC)
                .withDayOfMonth(1)
                .truncatedTo(ChronoUnit.DAYS)
                .toInstant()
                .toEpochMilli();
        long end = Instant.ofEpochMilli(start)
                .atZone(ZoneOffset.UTC)
                .plusMonths(1)
                .toInstant()
                .toEpochMilli();
        return new WindowBounds(start, end);
    }

    private static long eventTimestamp(FlightEvent event) {
        return event.getEventTime().toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    private static final class WindowBounds {
        private final long start;
        private final long end;

        private WindowBounds(long start, long end) {
            this.start = start;
            this.end = end;
        }
    }
}
