package it.uniroma2.sabd.replay.parser;

import it.uniroma2.sabd.model.FlightEvent;
import it.uniroma2.sabd.utils.EventTimeBuilder;

public class FlightCsvParser {

    public static FlightEvent parseLine(String line) {
        String[] values = line.split(",", -1);
        if (values.length < 10) return null;

        FlightEvent event = new FlightEvent();
        event.setYear(parseIntOrZero(values[0]));
        event.setMonth(parseIntOrZero(values[1]));
        event.setDayOfMonth(parseIntOrZero(values[2]));
        event.setCarrier(values[3]);
        event.setOriginAirportId(parseIntOrZero(values[4]));
        event.setCrsDepTime(parseIntOrZero(values[5]));
        event.setDepDelay(parseDoubleOrZero(values[6]));
        event.setCancelled(parseDoubleOrZero(values[7]));
        event.setDestAirportId(parseIntOrZero(values[8]));
        event.setDiverted(parseDoubleOrZero(values[9]));

        event.setEventTime(EventTimeBuilder.build(
                event.getYear(), event.getMonth(), event.getDayOfMonth(), event.getCrsDepTime()
        ));

        return event;
    }
    private FlightCsvParser() {}
    private static double parseDoubleOrZero(String value) {
        if (value == null || value.trim().isEmpty()) return 0.0;
        return Double.parseDouble(value.trim());
    }

    private static int parseIntOrZero(String value) {
        if (value == null || value.trim().isEmpty()) return 0;
        return Integer.parseInt(value.trim());
    }
}
