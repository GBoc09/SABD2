package it.uniroma2.sabd.export;

import java.util.ArrayList;
import java.util.List;

/**
 * Opzioni di export lette dalla riga di comando: --sort, --column nome=valore.
 */
public final class ExportOptions {

    private final boolean sortRows;
    private final List<StaticColumn> staticColumns;

    public ExportOptions(boolean sortRows, List<StaticColumn> staticColumns) {
        this.sortRows = sortRows;
        this.staticColumns = staticColumns;
    }

    public static ExportOptions parse(String[] args, int fromIndex) {
        boolean sortRows = false;
        List<StaticColumn> staticColumns = new ArrayList<>();

        for (int i = fromIndex; i < args.length; i++) {
            String option = args[i];
            if ("--sort".equals(option)) {
                sortRows = true;
            } else if ("--column".equals(option)) {
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("Opzione --column senza nome=valore");
                }
                staticColumns.add(StaticColumn.parse(args[++i]));
            } else {
                throw new IllegalArgumentException("Opzione non riconosciuta: " + option);
            }
        }

        return new ExportOptions(sortRows, staticColumns);
    }

    public boolean isSortRows() {
        return sortRows;
    }

    public List<StaticColumn> getStaticColumns() {
        return staticColumns;
    }
}