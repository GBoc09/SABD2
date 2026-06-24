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
        if (args.length < 3 || args.length > 4) {
            System.err.println("Uso: CSVExporter <source-dir> <dest-file.csv> <header> [--sort]");
            System.exit(1);
        }

        Path sourceDir = Paths.get(args[0]);
        Path destFile = Paths.get(args[1]);
        String header = args[2];
        boolean sortRows = args.length == 4 && "--sort".equals(args[3]);

        export(sourceDir, destFile, header, sortRows);
    }

    public static void export(Path sourceDir, Path destFile, String header, boolean sortRows) throws IOException {
        if (!Files.isDirectory(sourceDir)) {
            throw new IOException("Directory sorgente non trovata: " + sourceDir);
        }

        List<String> rows = readRows(sourceDir, header);
        if (sortRows) {
            rows.sort(rowComparator(header));
        }

        Path parent = destFile.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        Path tempFile = Files.createTempFile(parent, destFile.getFileName().toString(), ".tmp");
        try {
            writeCsv(tempFile, header, rows);
            moveAtomically(tempFile, destFile);
        } finally {
            Files.deleteIfExists(tempFile);
        }

        System.out.printf("CSV esportato: %s (%d righe)%n", destFile, rows.size());
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
        String[] leftValues = left.split(",", -1);
        String[] rightValues = right.split(",", -1);

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

    private static String valueAt(String[] values, int index) {
        if (index >= values.length) {
            return "";
        }
        return values[index];
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
}
