package it.uniroma2.sabd.export;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Parsing e ordinamento di righe CSV grezze (gestisce campi con virgole interne,
 * es. la colonna delayed_flights di Q2 che contiene "[(AA,123,45.00), ...]").
 */
final class CSVRowParser {

    private CSVRowParser() {}

    static List<String> parseRecord(String row) {
        List<String> values = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        int i = 0;
        while (i < row.length()) {
            char ch = row.charAt(i);

            if (ch == '"') {
                if (inQuotes && i + 1 < row.length() && row.charAt(i + 1) == '"') {
                    current.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (ch == ',' && !inQuotes) {
                values.add(current.toString());
                current.setLength(0);
            } else {
                current.append(ch);
            }

            i++;
        }

        values.add(current.toString());
        return values;
    }

    static Comparator<String> rowComparator(String header) {
        String[] columns = header.split(",", -1);
        int hourIndex = columnIndex(columns, "hour");

        if (hourIndex < 0) {
            return Comparator.naturalOrder();
        }

        return (left, right) -> compareRows(left, right, columns.length, hourIndex);
    }

    private static int columnIndex(String[] columns, String columnName) {
        for (int i = 0; i < columns.length; i++) {
            if (columnName.equals(columns[i])) {
                return i;
            }
        }
        return -1;
    }

    private static int compareRows(String left, String right, int columnCount, int hourIndex) {
        List<String> leftValues = parseRecord(left);
        List<String> rightValues = parseRecord(right);

        for (int i = 0; i < columnCount; i++) {
            String leftValue = valueAt(leftValues, i);
            String rightValue = valueAt(rightValues, i);

            int comparison = i == hourIndex
                    ? compareHour(leftValue, rightValue)
                    : leftValue.compareTo(rightValue);

            if (comparison != 0) {
                return comparison;
            }
        }

        return left.compareTo(right);
    }

    private static String valueAt(List<String> values, int index) {
        return index >= values.size() ? "" : values.get(index);
    }

    private static int compareHour(String left, String right) {
        try {
            return Integer.compare(Integer.parseInt(left.trim()), Integer.parseInt(right.trim()));
        } catch (NumberFormatException e) {
            return left.compareTo(right);
        }
    }
}
