package it.uniroma2.sabd.utils;

import java.time.LocalDateTime;

public class EventTimeBuilder {

    private EventTimeBuilder() {
    }

    public static LocalDateTime build(
            int year,
            int month,
            int day,
            int crsDepTime) {

        String hhmm =
                String.format("%04d", crsDepTime);

        int hour =
                Integer.parseInt(
                        hhmm.substring(0, 2));

        int minute =
                Integer.parseInt(
                        hhmm.substring(2, 4));

        return LocalDateTime.of(
                year,
                month,
                day,
                hour,
                minute
        );
    }
}
