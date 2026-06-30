#!/usr/bin/env bash
set -euo pipefail

COMPOSE_CMD="sudo docker compose --env-file docker/.env -f docker/docker-compose.yml"
SOURCE_CONTAINER="${SOURCE_CONTAINER:-taskmanager}"

OUTPUT_BASE="/opt/flink/output"
RESULT_DEST_DIR="${RESULT_DEST_DIR:-./output/discarded_tuples}"

EXPORTER_CLASSPATH="${EXPORTER_CLASSPATH:-target/classes:target/analisi-voli-1.0.0.jar}"
EXPORTER_CLASS="it.uniroma2.sabd.export.CSVExporter"

HEADER="query,window,event_time,event_timestamp_ms,window_start,window_end,carrier,origin_airport_id,dest_airport_id,crs_dep_time,dep_delay,cancelled,diverted,produced_at"

STAGING_BASE_DIR="$(mktemp -d)"
trap 'rm -rf "$STAGING_BASE_DIR"' EXIT

export_discarded_dataset() {
    local watermark="$1"
    local query="$2"
    local window="$3"

    local source_path="${OUTPUT_BASE}/${watermark}/discarded_tuples/${query}/${window}"
    local staging_dir="${STAGING_BASE_DIR}/${watermark}/${query}/${window}"
    local dest_file="${RESULT_DEST_DIR}/discarded_${query}_${window}_${watermark}.csv"

    mkdir -p "$staging_dir"
    mkdir -p "$RESULT_DEST_DIR"

    echo "Esporto scarti ${watermark}/${query}/${window} da ${SOURCE_CONTAINER}:${source_path}"

    if ! $COMPOSE_CMD cp "${SOURCE_CONTAINER}:${source_path}/." "$staging_dir" 2>/dev/null; then
        echo "Nessun output scarti per ${watermark}/${query}/${window}"
        return
    fi

    sudo chown -R "$(id -u):$(id -g)" "$staging_dir"

    java -cp "$EXPORTER_CLASSPATH" "$EXPORTER_CLASS" \
        "$staging_dir" \
        "$dest_file" \
        "$HEADER" \
        --sort \
        --column "watermark_strategy=${watermark}"

    echo "Salvato: $dest_file"
}

for WM in WM15 WM100 ADAPTIVE; do
    export_discarded_dataset "$WM" q1 1h
    export_discarded_dataset "$WM" q2 1h
    export_discarded_dataset "$WM" q2 6h
    export_discarded_dataset "$WM" q2 global
    export_discarded_dataset "$WM" q3 1day
    export_discarded_dataset "$WM" q3 7day
    export_discarded_dataset "$WM" q3 global
done

echo "Export scarti completato in: $RESULT_DEST_DIR"
