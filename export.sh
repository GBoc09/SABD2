#!/usr/bin/env bash
set -euo pipefail

COMPOSE_CMD="sudo docker compose --env-file docker/.env -f docker/docker-compose.yml"
SOURCE_CONTAINER="${SOURCE_CONTAINER:-taskmanager}"
SOURCE_PATH="${SOURCE_PATH:-/opt/flink/output/query1}"
DEST_DIR="${DEST_DIR:-./output/query1}"
HEADER="window_start,window_end,airline,num_flights,completed,cancelled,diverted,dep_delay_mean,cancellation_rate,late_departure_rate"

mkdir -p "$DEST_DIR"

echo "Esporto risultati Query1 da ${SOURCE_CONTAINER}:${SOURCE_PATH}"
$COMPOSE_CMD cp "${SOURCE_CONTAINER}:${SOURCE_PATH}/." "$DEST_DIR"

sudo chown -R "$(id -u):$(id -g)" "$DEST_DIR"

find "$DEST_DIR" -type f -size +0c | while read -r file; do
    first_line="$(head -n 1 "$file")"

    if [ "$first_line" != "$HEADER" ]; then
        tmp_file="$(mktemp)"
        {
            echo "$HEADER"
            cat "$file"
        } > "$tmp_file"
        mv "$tmp_file" "$file"
    fi
done

echo "Risultati esportati in: $DEST_DIR"
