package it.uniroma2.sabd.export;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;

/**
 * Scrive un CSV su disco in modo atomico: scrive su file temporaneo e poi
 * fa il rename, così un lettore concorrente non vede mai un file parziale.
 */
final class CSVRowWriter {

    private CSVRowWriter() {}

    static void writeAtomically(Path destFile, String header, List<String> rows) throws IOException {
        Path parent = destFile.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        assert parent != null;
        Path tempFile = Files.createTempFile(parent, destFile.getFileName().toString(), ".tmp");
        try {
            writeCsv(tempFile, header, rows);
            moveAtomically(tempFile, destFile);
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    private static void writeCsv(Path tempFile, String header, List<String> rows) throws IOException {
        try (BufferedWriter writer = Files.newBufferedWriter(
                tempFile, StandardCharsets.UTF_8, StandardOpenOption.TRUNCATE_EXISTING)) {
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
            // alcuni filesystem (es. overlay Docker) non supportano ATOMIC_MOVE
            Files.move(tempFile, destFile, StandardCopyOption.REPLACE_EXISTING);
        }
    }
}
