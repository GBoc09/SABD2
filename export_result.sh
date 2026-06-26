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

QUERY1_HEADER="window_start,window_end,airline,num_flights,completed,cancelled,diverted,dep_delay_mean,cancellation_rate,late_departure_rate"
QUERY2_HEADER="ts,rank,origin_airport_id,num_flights,severe_delays,dep_delay_mean,dep_delay_max,delayed_flights"
WINDOW_HEADER="ts,airline,hour,count,min,p25,p50,p75,p90,max"

# --- NUOVO HEADER PER LE TUPLE SCARTATE ---
DISCARDED_HEADER="window_type,origin_airport_id,carrier,event_time,window_start,window_end"

cleanup_output() {
    echo "Pulizia output CSV precedenti..."
    mkdir -p "${RESULT_DEST_DIR}"

    find "${RESULT_DEST_DIR}" -type f -name "*.csv" -delete 2>/dev/null || true

    mkdir -p "${RESULT_DEST_DIR}/query1"
    mkdir -p "${RESULT_DEST_DIR}/query2"
    mkdir -p "${RESULT_DEST_DIR}/query3"
    
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

export_query1() {
    for WM in WM15 WM100 ADAPTIVE; do
        export_dataset \
            "${OUTPUT_BASE}/${WM}/query1" \
            "${STAGING_BASE_DIR}/query1/${WM}" \
            "${RESULT_DEST_DIR}/query1/query1_${WM}.csv" \
            "$QUERY1_HEADER" \
            "Query1 [$WM]"
    done
}

export_query2() {
    local WM_LIST=("WM15" "WM100" "ADAPTIVE")
    local SUBDIRS=("1h" "6h" "global")

    for WM in "${WM_LIST[@]}"; do
        for SUB in "${SUBDIRS[@]}"; do
            export_dataset \
                "${OUTPUT_BASE}/${WM}/query2/${SUB}" \
                "${STAGING_BASE_DIR}/query2/${WM}/${SUB}" \
                "${RESULT_DEST_DIR}/query2/query2_${SUB}_${WM}.csv" \
                "$QUERY2_HEADER" \
                "Query2 [$WM/$SUB]"
        done
    done
}

export_query3() {
    local WM_LIST=("WM15" "WM100" "ADAPTIVE")
    local SUBDIRS=("1day" "7day" "global")

    for WM in "${WM_LIST[@]}"; do
        for SUB in "${SUBDIRS[@]}"; do
            export_dataset \
                "${OUTPUT_BASE}/${WM}/query3/${SUB}" \
                "${STAGING_BASE_DIR}/query3/${WM}/${SUB}" \
                "${RESULT_DEST_DIR}/query3/query3_${SUB}_${WM}.csv" \
                "$WINDOW_HEADER" \
                "Query3 [$WM/$SUB]"
        done
    done
}

# --- NUOVA FUNZIONE PER ESPORTARE I DATI FUORI ORDINE SCARTATI ---
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

# Esecuzione funzioni
cleanup_output
export_query1
export_query2
export_query3
export_query2_discarded # <--- CHIAMATA ALLA NUOVA FUNZIONE

echo "✔ EXPORT COMPLETATO"