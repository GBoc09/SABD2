package it.uniroma2.sabd.export;

/**
 * Coppia nome=valore aggiunta come colonna costante a ogni riga esportata
 * (es. watermark_strategy=WM15, parallelism=4).
 */
public final class StaticColumn {

    private final String name;
    private final String value;

    public StaticColumn(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public static StaticColumn parse(String raw) {
        int separator = raw.indexOf('=');
        if (separator <= 0) {
            throw new IllegalArgumentException("Colonna statica non valida: " + raw);
        }
        return new StaticColumn(raw.substring(0, separator), raw.substring(separator + 1));
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }
}
