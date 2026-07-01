package it.uniroma2.sabd.flink.utils;

import it.uniroma2.sabd.model.FlightEvent;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

public final class EventTimeUtils {

    private EventTimeUtils() {}

    public static final DateTimeFormatter CSV_FORMATTER = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.of("UTC"));

    public static long eventTimestamp(
            KeyedProcessFunction<?, FlightEvent, ?>.Context ctx,
            FlightEvent event) {
        Long timestamp = ctx.timestamp();
        if (timestamp != null) {
            return timestamp;
        }
        return event.getEventTime().toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    public static long monthStart(long epochMs) {
        return Instant.ofEpochMilli(epochMs)
                .atZone(ZoneOffset.UTC)
                .withDayOfMonth(1)
                .truncatedTo(ChronoUnit.DAYS)
                .toInstant()
                .toEpochMilli();
    }

    public static long nextMonthlyTimer(long epochMs) {
        return Instant.ofEpochMilli(epochMs)
                .atZone(ZoneOffset.UTC)
                .withDayOfMonth(1)
                .truncatedTo(ChronoUnit.DAYS)
                .plusMonths(1)
                .toInstant()
                .toEpochMilli();
    }

    public static long globalStart(long epochMs) {
        return Instant.ofEpochMilli(epochMs)
                .atZone(ZoneOffset.UTC)
                .withDayOfYear(1)
                .truncatedTo(ChronoUnit.DAYS)
                .toInstant()
                .toEpochMilli();
    }
}
