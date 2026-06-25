#!/usr/bin/env bash
set -euo pipefail

COMPOSE_CMD="sudo docker compose --env-file docker/.env -f docker/docker-compose.yml"

SOURCE_CONTAINER="${SOURCE_CONTAINER:-taskmanager}"

# Logs
LOG_SOURCE_PATH="/opt/flink/log"
LOG_DEST_DIR="./logs/taskmanager"
PERFORMANCE_SOURCE_PATH="${PERFORMANCE_SOURCE_PATH:-/opt/flink/performance}"
PERFORMANCE_DEST_DIR="${PERFORMANCE_DEST_DIR:-./performance}"

echo "Esporto i log da ${SOURCE_CONTAINER}:${LOG_SOURCE_PATH}"

rm -rf "$LOG_DEST_DIR"
mkdir -p "$LOG_DEST_DIR"

$COMPOSE_CMD cp "${SOURCE_CONTAINER}:${LOG_SOURCE_PATH}/." "$LOG_DEST_DIR"

sudo chown -R "$(id -u):$(id -g)" "$LOG_DEST_DIR"


# 1. LATENCY MONITOR
grep -h "LATENCY_MONITOR" "$LOG_DEST_DIR"/*.log \
    > "$LOG_DEST_DIR/latency_report.log" || true

# 2. THROUGHPUT MONITOR
grep -h "THROUGHPUT_MONITOR" "$LOG_DEST_DIR"/*.log \
    > "$LOG_DEST_DIR/throughput_report.log" || true

# 3. OUT OF ORDER REPORT
grep -h "OUT_OF_ORDER_REPORT" "$LOG_DEST_DIR"/*.log \
    > "$LOG_DEST_DIR/out_of_order_report.log" || true
echo "✓ Log esportati in: $LOG_DEST_DIR"

echo "Esporto metriche performance da ${SOURCE_CONTAINER}:${PERFORMANCE_SOURCE_PATH}"

rm -rf "$PERFORMANCE_DEST_DIR"
mkdir -p "$PERFORMANCE_DEST_DIR"

if $COMPOSE_CMD cp "${SOURCE_CONTAINER}:${PERFORMANCE_SOURCE_PATH}/." "$PERFORMANCE_DEST_DIR" 2>/dev/null; then
    sudo chown -R "$(id -u):$(id -g)" "$PERFORMANCE_DEST_DIR"
    echo "✓ Metriche performance esportate in: $PERFORMANCE_DEST_DIR"
else
    echo "Nessuna metrica performance trovata, salto export performance."
fi


echo ""
echo "Export log completato."
