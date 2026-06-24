#!/bin/bash
set -e

COMPOSE_CMD="sudo docker compose --env-file docker/.env -f docker/docker-compose.yml"

echo "=============================================="
echo "   Arresto Pipeline Streaming - SABD2         "
echo "=============================================="
echo ""

echo "Ricerca job Flink attivi..."

JOB_IDS=$($COMPOSE_CMD exec -T jobmanager \
    flink list 2>/dev/null | grep RUNNING | awk '{print $4}' || true)

for JOB_ID in $JOB_IDS; do
    echo "Terminazione job Flink: $JOB_ID"
    $COMPOSE_CMD exec -T jobmanager flink cancel "$JOB_ID"
done

echo ""
echo "Spegnimento infrastruttura..."

$COMPOSE_CMD down -v

echo ""
echo "Volumi rimossi e container arrestati."
echo "Spegnimento completato."