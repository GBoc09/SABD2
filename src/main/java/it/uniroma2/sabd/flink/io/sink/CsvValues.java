package it.uniroma2.sabd.flink.io.sink;

import java.util.Locale;

public final class CsvValues {

    private CsvValues() {
    }

    public static String text(String value) {
        if (value == null) {
            return "";
        }

        boolean mustQuote = value.contains(",")
                || value.contains("\"")
                || value.contains("\n")
                || value.contains("\r");

        if (!mustQuote) {
            return value;
        }

        return "\"" + value.replace("\"", "\"\"") + "\"";
    }

    public static String decimal(double value) {
        return String.format(Locale.US, "%.3f", value);
    }
}
