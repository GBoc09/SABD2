package it.uniroma2.sabd.export;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Entry point CLI per esportare l'output partizionato di Flink in un unico
 * CSV ordinato, con eventuali colonne statiche aggiunte (watermark, parallelismo).
 * Orchestrazione pura: lettura (CsvRowReader), ordinamento (CsvRowParser),
 * scrittura (CsvRowWriter) sono delegati a classi dedicate.
 */
public final class CSVExporter {

    private CSVExporter() {}

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.err.println("Uso: CSVExporter <source-dir> <dest-file.csv> <header> [--sort] [--column nome=valore]...");
            System.exit(1);
        }

        Path sourceDir = Paths.get(args[0]);
        Path destFile = Paths.get(args[1]);
        String header = args[2];
        ExportOptions options = ExportOptions.parse(args, 3);

        export(sourceDir, destFile, header, options.isSortRows(), options.getStaticColumns());
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

        List<String> rows = CSVRowReader.readRows(sourceDir, header);
        if (sortRows) {
            rows.sort(CSVRowParser.rowComparator(header));
        }

        rows = appendStaticColumns(rows, staticColumns);
        String outputHeader = appendStaticHeader(header, staticColumns);

        CSVRowWriter.writeAtomically(destFile, outputHeader, rows);

        System.out.printf("CSV esportato: %s (%d righe)%n", destFile, rows.size());
    }

    private static String appendStaticHeader(String header, List<StaticColumn> staticColumns) {
        if (staticColumns.isEmpty()) {
            return header;
        }
        String columnNames = staticColumns.stream()
                .map(StaticColumn::getName)
                .collect(Collectors.joining(","));
        return header + "," + columnNames;
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
}