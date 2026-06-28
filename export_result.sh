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


prepare_output_dirs() {
    echo "Preparo directory output CSV..."
    mkdir -p "${RESULT_DEST_DIR}"
    mkdir -p "${RESULT_DEST_DIR}/query1"
    mkdir -p "${RESULT_DEST_DIR}/query2"
    mkdir -p "${RESULT_DEST_DIR}/query3"
    
    echo "✔ Directory output pronte"
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


# Esecuzione funzioni
prepare_output_dirs
export_query1
export_query2
export_query3

echo "✔ EXPORT COMPLETATO"
