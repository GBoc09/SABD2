#!/bin/bash
set -e

COMPOSE_CMD="sudo docker compose --env-file docker/.env -f docker/docker-compose.yml"
KAFKA_TOPIC="flights"
KAFKA_PARTITIONS="2"

usage() {
    echo "Uso: $0"
    echo ""
    echo "Avvia l'infrastruttura core:"
    echo "  - NiFi"
    echo "  - HDFS"
    echo "  - Kafka"
    echo "  - Kafka UI"
    echo "  - Flink JobManager/TaskManager"
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
    usage
    exit 0
fi

if [ "$#" -ne 0 ]; then
    echo "Errore: deploy.sh non accetta parametri."
    usage
    exit 1
fi

echo "=============================================="
echo "   Avvio Infrastruttura Streaming - SABD2     "
echo "=============================================="
echo ""

echo "[1/4] Compilazione del progetto Java..."
if [ -f "pom.xml" ]; then
    mvn clean package -DskipTests
    echo "Build Maven completata."
    echo ""
else
    echo "File pom.xml non trovato, salto la build Maven."
    echo ""
fi

echo "[2/4] Pull e Build delle immagini Docker..."
$COMPOSE_CMD pull --ignore-pull-failures

echo "Forzando la build di NiFi senza cache..."
$COMPOSE_CMD build --no-cache nifi

echo "Build standard per gli altri servizi..."
$COMPOSE_CMD build --no-cache namenode datanode kafka kafka-ui jobmanager taskmanager replay grafana
echo "Immagini pronte."
echo ""

echo "[3/4] Avvio dell'infrastruttura core..."
$COMPOSE_CMD up -d nifi namenode datanode kafka kafka-ui jobmanager taskmanager grafana
echo "Container avviati."

wait_for_hdfs_file() {
    local file_path="$1"
    local max_attempts=60
    local sleep_seconds=5

    echo "Attendo che NiFi scriva il file su HDFS: $file_path"

    for attempt in $(seq 1 "$max_attempts"); do
        if $COMPOSE_CMD exec -T namenode hdfs dfs -test -s "$file_path"; then
            echo "File trovato su HDFS: $file_path"
            return 0
        fi

        echo "Tentativo $attempt/$max_attempts: file non ancora disponibile..."
        sleep "$sleep_seconds"
    done

    echo "Errore: file non trovato su HDFS dopo $((max_attempts * sleep_seconds)) secondi."
    return 1
}

wait_for_hdfs_file "/nifi_output/merge.csv"

echo "[4/4] Indirizzi dei servizi esposti:"
echo " - Kafka UI:      http://localhost:8080"
echo " - Hadoop (HDFS): http://localhost:9870"
echo " - Apache NiFi:   https://localhost:8443/nifi/login"
echo " - Apache Flink:  http://localhost:8081"
echo " - Apache Grafana:  http://localhost:3000"
echo ""

echo "Infrastruttura avviata con successo!"
echo "Per lanciare il job con WM15, WM100 e ADAPTIVE sullo stesso input:"
echo "  ./run_flink_job.sh"
