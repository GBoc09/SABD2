package it.uniroma2.sabd.export;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Legge ricorsivamente tutti i file di dati sotto una directory (output Flink
 * partizionato su più subtask) e ne estrae le righe, escludendo header e file
 * di sistema (_SUCCESS, .crc).
 */
final class CSVRowReader {

    private CSVRowReader() {}

    static List<String> readRows(Path sourceDir, String header) throws IOException {
        List<Path> files = listDataFiles(sourceDir);

        List<String> rows = new ArrayList<>();
        for (Path file : files) {
            if (Files.size(file) == 0L) {
                continue;
            }
            rows.addAll(readNonHeaderLines(file, header));
        }
        return rows;
    }

    private static List<Path> listDataFiles(Path sourceDir) throws IOException {
        try (Stream<Path> paths = Files.walk(sourceDir)) {
            return paths
                    .filter(Files::isRegularFile)
                    .filter(CSVRowReader::isDataFile)
                    .sorted()
                    .collect(Collectors.toList());
        }
    }

    private static List<String> readNonHeaderLines(Path file, String header) throws IOException {
        List<String> lines = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isBlank() && !line.equals(header)) {
                    lines.add(line);
                }
            }
        }
        return lines;
    }

    private static boolean isDataFile(Path file) {
        String fileName = file.getFileName().toString();
        return !fileName.startsWith("_") && !fileName.endsWith(".crc");
    }
}
