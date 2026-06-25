#!/usr/bin/env bash
set -euo pipefail

COMPOSE_CMD="sudo docker compose --env-file docker/.env -f docker/docker-compose.yml"

SOURCE_CONTAINER="${SOURCE_CONTAINER:-taskmanager}"

OUTPUT_BASE="/opt/flink/output"
RESULT_DEST_DIR="${RESULT_DEST_DIR:-./output}"

EXPORTER_CLASSPATH="target/classes:target/analisi-voli-1.0.0.jar"
EXPORTER_CLASS="it.uniroma2.sabd.export.CSVExporter"

QUERY1_HEADER="window_start,window_end,airline,num_flights,completed,cancelled,diverted,dep_delay_mean,cancellation_rate,late_departure_rate"
WINDOW_HEADER="ts,airline,hour,count,min,p25,p50,p75,p90,max"
GLOBAL_HEADER="global_start,snapshot_ts,airline,hour,count,min,p25,p50,p75,p90,max"

STAGING_BASE_DIR="$(mktemp -d)"
trap 'rm -rf "$STAGING_BASE_DIR"' EXIT


# ------------------------------------------------------------
# QUERY1 EXPORT (WM15 / WM30 / ADAPTIVE)
# ------------------------------------------------------------
export_query1() {

    for WM in WM15 WM30 ADAPTIVE; do

        local source_path="${OUTPUT_BASE}/${WM}/query1"
        local staging_dir="${STAGING_BASE_DIR}/query1/${WM}"
        local dest_file="${RESULT_DEST_DIR}/query1_${WM}.csv"

        mkdir -p "$staging_dir"
        mkdir -p "$RESULT_DEST_DIR"

        echo "Esporto Query1 [$WM] da ${SOURCE_CONTAINER}:${source_path}"

        if ! $COMPOSE_CMD cp "${SOURCE_CONTAINER}:${source_path}/." "$staging_dir" 2>/dev/null; then
            echo "⚠ Nessun output per Query1 [$WM]"
            continue
        fi

        sudo chown -R "$(id -u):$(id -g)" "$staging_dir"

        java -cp "$EXPORTER_CLASSPATH" "$EXPORTER_CLASS" \
            "$staging_dir" \
            "$dest_file" \
            "$QUERY1_HEADER" \
            --sort

        echo "✔ Query1 [$WM] salvato in: $dest_file"
    done
}


# ------------------------------------------------------------
# QUERY3 EXPORT (1day / 7day / global)
# ------------------------------------------------------------
export_query3() {

    local WM_LIST=("WM15" "WM30" "ADAPTIVE")
    local SUBDIRS=("1day" "7day" "global")

    for WM in "${WM_LIST[@]}"; do
        for SUB in "${SUBDIRS[@]}"; do

            local source_path="${OUTPUT_BASE}/${WM}/query3/${SUB}"
            local staging_dir="${STAGING_BASE_DIR}/query3/${WM}/${SUB}"
            local dest_file="${RESULT_DEST_DIR}/query3_${SUB}_${WM}.csv"

            mkdir -p "$staging_dir"
            mkdir -p "$RESULT_DEST_DIR"

            echo "Esporto Query3 [$WM/$SUB] da ${SOURCE_CONTAINER}:${source_path}"

            if ! $COMPOSE_CMD cp "${SOURCE_CONTAINER}:${source_path}/." "$staging_dir" 2>/dev/null; then
                echo "⚠ Nessun output Query3 [$WM/$SUB]"
                continue
            fi

            sudo chown -R "$(id -u):$(id -g)" "$staging_dir"

            HEADER="$WINDOW_HEADER"
            if [ "$SUB" == "global" ]; then
                HEADER="$GLOBAL_HEADER"
            fi

            java -cp "$EXPORTER_CLASSPATH" "$EXPORTER_CLASS" \
                "$staging_dir" \
                "$dest_file" \
                "$HEADER" \
                --sort

            echo "✔ Query3 [$WM/$SUB] salvato in: $dest_file"
        done
    done
}


export_query1
export_query3

echo " EXPORT COMPLETATO"