#!/bin/bash
set -e

COMPOSE_CMD="sudo docker compose --env-file docker/.env -f docker/docker-compose.yml"

echo "=============================================="
echo "   Arresto Pipeline Streaming - SABD2         "
echo "=============================================="
echo ""


echo "Rimozione dei volumi e spegnimento container ..."
$COMPOSE_CMD down -v
echo "Volumi rimossi e container disabilitati. "
echo ""

echo "Spegnimento completato."