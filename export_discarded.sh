#!/usr/bin/env bash
set -euo pipefail

COMPOSE_CMD="sudo docker compose --env-file docker/.env -f docker/docker-compose.yml"

SOURCE_CONTAINER="${SOURCE_CONTAINER:-taskmanager}"

OUTPUT_BASE="/opt/flink/output"
RESULT_DEST_DIR="${RESULT_DEST_DIR:-./output}"

EXPORTER_CLASSPATH="target/classes:target/analisi-voli-1.0.0.jar"
EXPORTER_CLASS="it.uniroma2.sabd.export.CSVExporter"

STAGING_BASE_DIR="$(mktemp -d)"
trap 'rm -rf "$STAGING_BASE_DIR"' EXIT

QUERY2_HEADER="ts,rank,origin_airport_id,num_flights,severe_delays,dep_delay_mean,dep_delay_max,delayed_flights"

# --- NUOVO HEADER PER LE TUPLE SCARTATE ---
DISCARDED_HEADER="window_type,origin_airport_id,carrier,event_time,window_start,window_end"

cleanup_output() {
    echo "Pulizia output CSV precedenti..."
    mkdir -p "${RESULT_DEST_DIR}"

    find "${RESULT_DEST_DIR}" -type f -name "*.csv" -delete 2>/dev/null || true

    mkdir -p "${RESULT_DEST_DIR}/query2"
    
    # Crea la cartella per i report dei dati scartati
    mkdir -p "${RESULT_DEST_DIR}/discarded"

    echo "✔ Output pulito"
}

export_dataset() {
    local source_path="$1"
    local staging_dir="$2"
    local dest_file="$3"
    local header="$4"
    local label="$5"

    mkdir -p "$staging_dir"
    mkdir -p "$(dirname "$dest_file")"

    echo "Esporto ${label} da ${SOURCE_CONTAINER}:${source_path}"

    if ! $COMPOSE_CMD cp "${SOURCE_CONTAINER}:${source_path}/." "$staging_dir" 2>/dev/null; then
        echo "⚠ Nessun output per ${label}"
        return
    fi

    sudo chown -R "$(id -u):$(id -g)" "$staging_dir"

    java -cp "$EXPORTER_CLASSPATH" "$EXPORTER_CLASS" \
        "$staging_dir" \
        "$dest_file" \
        "$header" \
        --sort

    echo "✔ Salvato: $dest_file"
}


export_query2_discarded() {
    # Poiché raccogliamo gli scarti solo per WM15, esportiamo solo quel blocco
    local WM="WM15"
    
    # Esporta scarti 1h
    export_dataset \
        "${OUTPUT_BASE}/${WM}/query2/discarded_1h" \
        "${STAGING_BASE_DIR}/discarded/${WM}/1h" \
        "${RESULT_DEST_DIR}/discarded/query2_discarded_1h_${WM}.csv" \
        "$DISCARDED_HEADER" \
        "Query2 Discarded 1h [$WM]"

    # Esporta scarti 6h
    export_dataset \
        "${OUTPUT_BASE}/${WM}/query2/discarded_6h" \
        "${STAGING_BASE_DIR}/discarded/${WM}/6h" \
        "${RESULT_DEST_DIR}/discarded/query2_discarded_6h_${WM}.csv" \
        "$DISCARDED_HEADER" \
        "Query2 Discarded 6h [$WM]"
}
export_query2_discarded # <--- CHIAMATA ALLA NUOVA FUNZIONE

echo "✔ EXPORT COMPLETATO"