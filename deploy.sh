#!/bin/bash
set -e

COMPOSE_CMD="docker compose --env-file docker/.env -f docker/docker-compose.yml"

echo "=============================================="
echo "   Avvio Pipeline Streaming - SABD2           "
echo "=============================================="
echo ""

echo "[1/5] Compilazione del progetto Java..."
if [ -f "pom.xml" ]; then
    mvn clean package -DskipTests
    echo "Build Maven completata."
    echo ""
else
    echo "File pom.xml non trovato, salto la build Maven."
    echo ""
fi

echo "[2/5] Pull e Build delle immagini Docker..."
$COMPOSE_CMD pull --ignore-pull-failures

echo "Forzando la build di NiFi senza cache..."
$COMPOSE_CMD build --no-cache nifi

echo "Build standard per gli altri servizi..."
$COMPOSE_CMD build
echo "Immagini pronte."
echo ""

echo "[3/5] Avvio dell'infrastruttura core..."
$COMPOSE_CMD up -d nifi namenode datanode kafka kafka-ui jobmanager taskmanager
echo "Container core avviati."
echo ""

echo "[4/5] Indirizzi dei servizi esposti:"
echo " - Kafka UI:      http://localhost:8080"
echo " - Hadoop (HDFS): http://localhost:9870"
echo " - Apache NiFi:   https://localhost:8443/nifi/login"
echo " - Apache Flink:  http://localhost:8081"
echo ""

echo "[5/5] Istruzioni per il Replay Engine..."
echo "L'infrastruttura di base è pronta. Lascia a NiFi il tempo di elaborare"
echo "i file CSV e scriverli su HDFS nella cartella /nifi_output/merge.csv."
echo ""
echo "Quando vedi che i dati su HDFS sono pronti, lancia QUESTO comando"
echo "per ricompilare l'immagine con il nuovo .jar e far partire il replay:"
echo "----------------------------------------------------------------------"
echo "sudo $COMPOSE_CMD up --build replay"
echo "----------------------------------------------------------------------"
echo ""
echo "Infrastruttura avviata con successo!"