package it.uniroma2.sabd.export;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class CSVExporter {

    private CSVExporter() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.err.println("Uso: CSVExporter <source-dir> <dest-file.csv> <header> [--sort] [--column nome=valore]...");
            System.exit(1);
        }

        Path sourceDir = Paths.get(args[0]);
        Path destFile = Paths.get(args[1]);
        String header = args[2];
        ExportOptions options = parseOptions(args);

        export(sourceDir, destFile, header, options.sortRows, options.staticColumns);
    }

    public static void export(Path sourceDir, Path destFile, String header, boolean sortRows) throws IOException {
        export(sourceDir, destFile, header, sortRows, List.of());
    }

    public static void export(
            Path sourceDir,
            Path destFile,
            String header,
            boolean sortRows,
            List<StaticColumn> staticColumns) throws IOException {
        if (!Files.isDirectory(sourceDir)) {
            throw new IOException("Directory sorgente non trovata: " + sourceDir);
        }

        List<String> rows = readRows(sourceDir, header);
        if (sortRows) {
            rows.sort(rowComparator(header));
        }
        rows = appendStaticColumns(rows, staticColumns);
        String outputHeader = appendStaticHeader(header, staticColumns);

        Path parent = destFile.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        Path tempFile = Files.createTempFile(parent, destFile.getFileName().toString(), ".tmp");
        try {
            writeCsv(tempFile, outputHeader, rows);
            moveAtomically(tempFile, destFile);
        } finally {
            Files.deleteIfExists(tempFile);
        }

        System.out.printf("CSV esportato: %s (%d righe)%n", destFile, rows.size());
    }

    private static ExportOptions parseOptions(String[] args) {
        boolean sortRows = false;
        List<StaticColumn> staticColumns = new ArrayList<>();

        for (int i = 3; i < args.length; i++) {
            String option = args[i];
            if ("--sort".equals(option)) {
                sortRows = true;
            } else if ("--column".equals(option)) {
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("Opzione --column senza nome=valore");
                }
                staticColumns.add(parseStaticColumn(args[++i]));
            } else {
                throw new IllegalArgumentException("Opzione non riconosciuta: " + option);
            }
        }

        return new ExportOptions(sortRows, staticColumns);
    }

    private static StaticColumn parseStaticColumn(String column) {
        int separator = column.indexOf('=');
        if (separator <= 0) {
            throw new IllegalArgumentException("Colonna statica non valida: " + column);
        }

        return new StaticColumn(
                column.substring(0, separator),
                column.substring(separator + 1));
    }

    private static String appendStaticHeader(String header, List<StaticColumn> staticColumns) {
        if (staticColumns.isEmpty()) {
            return header;
        }

        List<String> columnNames = staticColumns.stream()
                .map(StaticColumn::getName)
                .collect(Collectors.toList());
        return header + "," + String.join(",", columnNames);
    }

    private static List<String> appendStaticColumns(List<String> rows, List<StaticColumn> staticColumns) {
        if (staticColumns.isEmpty()) {
            return rows;
        }

        String suffix = staticColumns.stream()
                .map(StaticColumn::getValue)
                .collect(Collectors.joining(","));

        return rows.stream()
                .map(row -> row + "," + suffix)
                .collect(Collectors.toList());
    }

    private static List<String> readRows(Path sourceDir, String header) throws IOException {
        List<Path> files;
        try (Stream<Path> paths = Files.walk(sourceDir)) {
            files = paths
                    .filter(Files::isRegularFile)
                    .filter(CSVExporter::isDataFile)
                    .sorted()
                    .collect(Collectors.toList());
        }

        List<String> rows = new ArrayList<>();
        for (Path file : files) {
            if (Files.size(file) == 0L) {
                continue;
            }

            try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (!line.isBlank() && !line.equals(header)) {
                        rows.add(line);
                    }
                }
            }
        }
        return rows;
    }

    private static boolean isDataFile(Path file) {
        String fileName = file.getFileName().toString();
        return !fileName.startsWith("_") && !fileName.endsWith(".crc");
    }

    private static Comparator<String> rowComparator(String header) {
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
        List<String> leftValues = parseCsvRecord(left);
        List<String> rightValues = parseCsvRecord(right);

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

    private static List<String> parseCsvRecord(String row) {
        List<String> values = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < row.length(); i++) {
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
        }

        values.add(current.toString());
        return values;
    }

    private static String valueAt(List<String> values, int index) {
        if (index >= values.size()) {
            return "";
        }
        return values.get(index);
    }

    private static int compareHour(String left, String right) {
        try {
            return Integer.compare(Integer.parseInt(left.trim()), Integer.parseInt(right.trim()));
        } catch (NumberFormatException e) {
            return left.compareTo(right);
        }
    }

    private static void writeCsv(Path tempFile, String header, List<String> rows) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(
                tempFile,
                StandardCharsets.UTF_8,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            writer.write(header);
            writer.newLine();

            for (String row : rows) {
                writer.write(row);
                writer.newLine();
            }
        }
    }

    private static void moveAtomically(Path tempFile, Path destFile) throws IOException {
        try {
            Files.move(tempFile, destFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            Files.move(tempFile, destFile, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    public static final class StaticColumn {
        private final String name;
        private final String value;

        public StaticColumn(String name, String value) {
            this.name = name;
            this.value = value;
        }

        public String getName() {
            return name;
        }

        public String getValue() {
            return value;
        }
    }

    private static final class ExportOptions {
        private final boolean sortRows;
        private final List<StaticColumn> staticColumns;

        private ExportOptions(boolean sortRows, List<StaticColumn> staticColumns) {
            this.sortRows = sortRows;
            this.staticColumns = staticColumns;
        }
    }
}
