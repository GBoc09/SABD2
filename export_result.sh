#!/usr/bin/env bash
set -euo pipefail

COMPOSE_CMD="sudo docker compose --env-file docker/.env -f docker/docker-compose.yml"

SOURCE_CONTAINER="${SOURCE_CONTAINER:-taskmanager}"
QUERY1_SOURCE_PATH="${QUERY1_SOURCE_PATH:-${SOURCE_PATH:-/opt/flink/output/query1}}"
QUERY3_SOURCE_BASE_PATH="${QUERY3_SOURCE_BASE_PATH:-${SOURCE_BASE_PATH:-/opt/flink/output/query3}}"
RESULT_DEST_DIR="${RESULT_DEST_DIR:-./output}"
QUERY1_DEST_FILE="${QUERY1_DEST_FILE:-${DEST_FILE:-${RESULT_DEST_DIR}/query1.csv}}"
QUERY3_DEST_BASE_DIR="${QUERY3_DEST_BASE_DIR:-${DEST_BASE_DIR:-${RESULT_DEST_DIR}/query3}}"
EXPORTER_CLASSPATH="${EXPORTER_CLASSPATH:-target/classes:target/analisi-voli-1.0.0.jar}"
EXPORTER_CLASS="it.uniroma2.sabd.export.CSVExporter"

QUERY1_HEADER="window_start,window_end,airline,num_flights,completed,cancelled,diverted,dep_delay_mean,cancellation_rate,late_departure_rate"
WINDOW_HEADER="ts,airline,hour,count,min,p25,p50,p75,p90,max"
GLOBAL_HEADER="global_start,snapshot_ts,airline,hour,count,min,p25,p50,p75,p90,max"
STAGING_BASE_DIR="$(mktemp -d)"
trap 'rm -rf "$STAGING_BASE_DIR"' EXIT

export_query1() {
    local staging_dir="${STAGING_BASE_DIR}/query1"

    mkdir -p "$staging_dir"
    mkdir -p "$(dirname "$QUERY1_DEST_FILE")"

    echo "Esporto Query1 da ${SOURCE_CONTAINER}:${QUERY1_SOURCE_PATH}"
    if ! $COMPOSE_CMD cp "${SOURCE_CONTAINER}:${QUERY1_SOURCE_PATH}/." "$staging_dir"; then
        echo "Nessun output trovato per Query1, salto."
        return
    fi

    sudo chown -R "$(id -u):$(id -g)" "$staging_dir"
    java -cp "$EXPORTER_CLASSPATH" "$EXPORTER_CLASS" "$staging_dir" "$QUERY1_DEST_FILE" "$QUERY1_HEADER" --sort

    echo "Risultati Query1 esportati in: $QUERY1_DEST_FILE"
}

export_query3_sink() {
    local source_subdir="$1"
    local dest_file_name="$2"
    local header="$3"

    local source_path="${QUERY3_SOURCE_BASE_PATH}/${source_subdir}"
    local staging_dir="${STAGING_BASE_DIR}/query3/${source_subdir}"
    local dest_file="${QUERY3_DEST_BASE_DIR}/${dest_file_name}"

    mkdir -p "$staging_dir"
    mkdir -p "$QUERY3_DEST_BASE_DIR"

    echo "Esporto Query3 ${source_subdir} da ${SOURCE_CONTAINER}:${source_path}"
    if ! $COMPOSE_CMD cp "${SOURCE_CONTAINER}:${source_path}/." "$staging_dir"; then
        echo "Nessun output trovato per Query3 ${source_subdir}, salto."
        return
    fi

    sudo chown -R "$(id -u):$(id -g)" "$staging_dir"
    java -cp "$EXPORTER_CLASSPATH" "$EXPORTER_CLASS" "$staging_dir" "$dest_file" "$header" --sort

    echo "Risultati Query3 ${source_subdir} esportati in: $dest_file"
}

export_query1
export_query3_sink "1day" "1day.csv" "$WINDOW_HEADER"
export_query3_sink "7day" "7day.csv" "$WINDOW_HEADER"
export_query3_sink "global" "global.csv" "$GLOBAL_HEADER"

echo ""
echo "Export risultati completato."
