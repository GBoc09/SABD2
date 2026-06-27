#!/usr/bin/env bash
set -euo pipefail

COMPOSE_CMD="sudo docker compose --env-file docker/.env -f docker/docker-compose.yml"
SOURCE_CONTAINER="${SOURCE_CONTAINER:-taskmanager}"
OUTPUT_BASE="/opt/flink/output"
RESULT_DEST_DIR="${RESULT_DEST_DIR:-./output/watermark_late_events}"
EXPORTER_CLASSPATH="${EXPORTER_CLASSPATH:-target/classes:target/analisi-voli-1.0.0.jar}"
EXPORTER_CLASS="it.uniroma2.sabd.export.CSVExporter"
HEADER="event_time,event_timestamp_ms,current_watermark,current_watermark_ms,lateness_ms,carrier,origin_airport_id,dest_airport_id,crs_dep_time,dep_delay,cancelled,diverted,produced_at"

STAGING_BASE_DIR="$(mktemp -d)"
trap 'rm -rf "$STAGING_BASE_DIR"' EXIT

export_late_events() {
    local watermark="$1"
    local source_path="${OUTPUT_BASE}/${watermark}/watermark_late_events"
    local staging_dir="${STAGING_BASE_DIR}/${watermark}"
    local dest_file="${RESULT_DEST_DIR}/late_after_watermark_${watermark}.csv"

    mkdir -p "$staging_dir"
    mkdir -p "$RESULT_DEST_DIR"

    echo "Esporto late-after-watermark ${watermark} da ${SOURCE_CONTAINER}:${source_path}"

    if ! $COMPOSE_CMD cp "${SOURCE_CONTAINER}:${source_path}/." "$staging_dir" 2>/dev/null; then
        echo "Nessun output late-after-watermark per ${watermark}"
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
    export_late_events "$WM"
done

echo "Export late-after-watermark completato in: $RESULT_DEST_DIR"
