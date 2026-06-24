#!/usr/bin/env bash
set -euo pipefail

COMPOSE_CMD="sudo docker compose --env-file docker/.env -f docker/docker-compose.yml"

SOURCE_CONTAINER="${SOURCE_CONTAINER:-taskmanager}"
SOURCE_BASE_PATH="${SOURCE_BASE_PATH:-/opt/flink/output/query3}"
DEST_BASE_DIR="${DEST_BASE_DIR:-./output/query3}"

WINDOW_HEADER="ts,airline,hour,count,min,p25,p50,p75,p90,max"
GLOBAL_HEADER="global_start,snapshot_ts,airline,hour,count,min,p25,p50,p75,p90,max"

copy_sink() {
    local source_subdir="$1"
    local dest_subdir="$2"
    local header="$3"

    local source_path="${SOURCE_BASE_PATH}/${source_subdir}"
    local dest_dir="${DEST_BASE_DIR}/${dest_subdir}"

    mkdir -p "$dest_dir"

    echo "Esporto Query3 ${source_subdir} da ${SOURCE_CONTAINER}:${source_path}"
    if ! $COMPOSE_CMD cp "${SOURCE_CONTAINER}:${source_path}/." "$dest_dir"; then
        echo "Nessun output trovato per Query3 ${source_subdir}, salto."
        return
    fi

    sudo chown -R "$(id -u):$(id -g)" "$dest_dir"
    add_header "$dest_dir" "$header"

    echo "Risultati Query3 ${source_subdir} esportati in: $dest_dir"
}

add_header() {
    local dest_dir="$1"
    local header="$2"

    find "$dest_dir" -type f -size +0c | while IFS= read -r file; do
        local first_line
        first_line="$(head -n 1 "$file")"

        if [ "$first_line" != "$header" ]; then
            local tmp_file
            tmp_file="$(mktemp)"
            {
                echo "$header"
                cat "$file"
            } > "$tmp_file"
            mv "$tmp_file" "$file"
        fi
    done
}

copy_sink "1day" "1day" "$WINDOW_HEADER"
copy_sink "7day" "7day" "$WINDOW_HEADER"
copy_sink "global" "global" "$GLOBAL_HEADER"

echo ""
echo "Export Query3 completato."
