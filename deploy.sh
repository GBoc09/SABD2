#!/bin/bash
set -e

# Creiamo una variabile che include sia il compose file che il file .env
COMPOSE_CMD="docker compose --env-file docker/.env -f docker/docker-compose.yml"

echo "=============================================="
echo "   Avvio Pipeline Streaming - SABD2           "
echo "=============================================="
echo ""

echo "[1/6] Compilazione del progetto Java..."
if [ -f "pom.xml" ]; then
    mvn clean package -DskipTests
    echo "Build Maven completata."
    echo ""
else
    echo "File pom.xml non trovato, salto la build Maven."
    echo ""
fi

echo "[2/6] Pull e Build delle immagini Docker..."
$COMPOSE_CMD pull --ignore-pull-failures
$COMPOSE_CMD build
echo "Immagini pronte."
echo ""

echo "[3/6] Avvio dell'infrastruttura core..."
$COMPOSE_CMD up -d nifi namenode datanode kafka kafka-ui jobmanager taskmanager
echo "Container core avviati."
echo ""

echo "[4/6] Indirizzi dei servizi esposti:"
echo " - Kafka UI:      http://localhost:8080"
echo " - Hadoop (HDFS): http://localhost:9870"
echo " - Apache NiFi:   https://localhost:8443/nifi"
echo " - Apache Flink:  http://localhost:8081"
echo ""

echo "[5/6] In attesa che NiFi scriva i dati su HDFS..."
MAX_RETRIES=60
RETRY_COUNT=0
FILE_READY=false

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if docker exec namenode hdfs dfs -test -e /nifi_output/merge.csv > /dev/null 2>&1; then
        FILE_READY=true
        break
    fi
    echo -n "."
    sleep 120
    ((RETRY_COUNT++))
done
echo ""

if [ "$FILE_READY" = true ]; then
    echo "File /nifi_output/merge.csv trovato in HDFS!"
    echo ""
else
    echo "ERRORE: Timeout. Il file non è stato generato su HDFS."
    exit 1
fi

echo "[6/6] Avvio del Replay Engine..."
$COMPOSE_CMD up -d replay
echo "Container replay avviato con successo."
echo ""
echo "Pipeline avviata! Puoi controllare i log con: docker logs -f replay"