#!/usr/bin/env bash
set -euo pipefail

COMPOSE_CMD="sudo docker compose --env-file docker/.env -f docker/docker-compose.yml"

SOURCE_CONTAINER="${SOURCE_CONTAINER:-taskmanager}"

# Logs
LOG_SOURCE_PATH="/opt/flink/log"
LOG_DEST_DIR="./logs/taskmanager"
PERFORMANCE_SOURCE_PATH="${PERFORMANCE_SOURCE_PATH:-/opt/flink/performance}"
PERFORMANCE_DEST_DIR="${PERFORMANCE_DEST_DIR:-./performance}"
EXPORTER_CLASSPATH="${EXPORTER_CLASSPATH:-target/classes:target/analisi-voli-1.0.0.jar}"
EXPORTER_CLASS="it.uniroma2.sabd.export.CSVExporter"
PERFORMANCE_PARALLELISM="${PERFORMANCE_PARALLELISM:-${FLINK_PARALLELISM:-}}"
LOG_STAGING_DIR="$(mktemp -d)"
PERFORMANCE_STAGING_DIR="$(mktemp -d)"
trap 'rm -rf "$LOG_STAGING_DIR" "$PERFORMANCE_STAGING_DIR"' EXIT

LATENCY_HEADER="timestamp_ms,label,source_subtask_index,window_start_ms,window_end_ms,window_duration_ms,window_events,total_events,min_latency_ms,max_latency_ms,avg_latency_ms"
THROUGHPUT_HEADER="timestamp_ms,label,source_subtask_index,window_start_ms,window_end_ms,window_duration_ms,window_events,total_events,instant_throughput_events_per_second,average_throughput_events_per_second"

usage() {
    echo "Uso: $0 --parallelism N"
    echo ""
    echo "Esempi:"
    echo "  $0 --parallelism 2"
    echo "  PERFORMANCE_PARALLELISM=2 $0"
    echo ""
    echo "Il parallelismo e' richiesto per non etichettare i CSV con un valore implicito o vecchio."
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
    usage
    exit 0
fi

while [ "$#" -gt 0 ]; do
    case "$1" in
        --parallelism)
            if [ "$#" -lt 2 ]; then
                echo "Errore: --parallelism richiede un valore."
                usage
                exit 1
            fi
            PERFORMANCE_PARALLELISM="$2"
            shift 2
            ;;
        *)
            echo "Errore: parametro non valido: $1"
            usage
            exit 1
            ;;
    esac
done

if [ -z "$PERFORMANCE_PARALLELISM" ]; then
    echo "Errore: specifica il parallelismo usato dalla run esportata."
    usage
    exit 1
fi

case "$PERFORMANCE_PARALLELISM" in
    ""|*[!0-9]*)
        echo "Errore: --parallelism deve essere un intero positivo."
        usage
        exit 1
        ;;
esac

if [ "$PERFORMANCE_PARALLELISM" -lt 1 ]; then
    echo "Errore: --parallelism deve essere almeno 1."
    usage
    exit 1
fi

echo "Esporto i log da ${SOURCE_CONTAINER}:${LOG_SOURCE_PATH}"
echo "Parallelismo metriche: ${PERFORMANCE_PARALLELISM}"

mkdir -p "$LOG_DEST_DIR"

$COMPOSE_CMD cp "${SOURCE_CONTAINER}:${LOG_SOURCE_PATH}/." "$LOG_STAGING_DIR"

sudo chown -R "$(id -u):$(id -g)" "$LOG_STAGING_DIR"
cp -a "$LOG_STAGING_DIR/." "$LOG_DEST_DIR"


# 1. LATENCY MONITOR
grep -h "LATENCY_MONITOR" "$LOG_STAGING_DIR"/*.log \
    > "$LOG_DEST_DIR/latency_report.log" || true

# 2. THROUGHPUT MONITOR
grep -h "THROUGHPUT_MONITOR" "$LOG_STAGING_DIR"/*.log \
    > "$LOG_DEST_DIR/throughput_report.log" || true

# 3. OUT OF ORDER REPORT
grep -h "OUT_OF_ORDER_REPORT" "$LOG_STAGING_DIR"/*.log \
    > "$LOG_DEST_DIR/out_of_order_report.log" || true
echo "✓ Log esportati in: $LOG_DEST_DIR"

echo "Esporto metriche performance da ${SOURCE_CONTAINER}:${PERFORMANCE_SOURCE_PATH}"

mkdir -p "$PERFORMANCE_DEST_DIR"

export_performance_csv() {
    local watermark="$1"
    local metric="$2"
    local header="$3"
    local source_dir="${PERFORMANCE_STAGING_DIR}/${watermark}/${metric}"
    local dest_file="${PERFORMANCE_DEST_DIR}/${metric}_${watermark}_p${PERFORMANCE_PARALLELISM}.csv"

    if [ ! -d "$source_dir" ]; then
        echo "Nessuna metrica ${metric} per ${watermark}"
        return
    fi

    java -cp "$EXPORTER_CLASSPATH" "$EXPORTER_CLASS" \
        "$source_dir" \
        "$dest_file" \
        "$header" \
        --sort \
        --column "watermark_strategy=${watermark}" \
        --column "parallelism=${PERFORMANCE_PARALLELISM}"

    echo "✓ Salvato: $dest_file"
}

if $COMPOSE_CMD cp "${SOURCE_CONTAINER}:${PERFORMANCE_SOURCE_PATH}/." "$PERFORMANCE_STAGING_DIR" 2>/dev/null; then
    sudo chown -R "$(id -u):$(id -g)" "$PERFORMANCE_STAGING_DIR"

    for WM in WM15 WM100 ADAPTIVE; do
        export_performance_csv "$WM" "latency" "$LATENCY_HEADER"
        export_performance_csv "$WM" "throughput" "$THROUGHPUT_HEADER"
    done

    echo "✓ Metriche performance esportate in CSV in: $PERFORMANCE_DEST_DIR"
else
    echo "Nessuna metrica performance trovata, salto export performance."
fi


echo ""
echo "Export log completato."
