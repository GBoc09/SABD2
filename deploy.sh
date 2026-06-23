#!/bin/bash
set -e

COMPOSE_CMD="sudo docker compose --env-file docker/.env -f docker/docker-compose.yml"
KAFKA_TOPIC="${KAFKA_TOPIC:-flights}"
KAFKA_PARTITIONS="${KAFKA_PARTITIONS:-4}"

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

echo "Forzando la build di NiFi senza cache..."
$COMPOSE_CMD build --no-cache nifi

echo "Build standard per gli altri servizi..."
$COMPOSE_CMD build --no-cache namenode datanode kafka kafka-ui jobmanager taskmanager replay
echo "Immagini pronte."
echo ""

echo "[3/6] Avvio dell'infrastruttura core..."
$COMPOSE_CMD up -d nifi namenode datanode kafka kafka-ui jobmanager taskmanager
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

wait_for_kafka() {
    local max_attempts=30
    local sleep_seconds=2

    echo "Attendo che Kafka sia pronto..."

    for attempt in $(seq 1 "$max_attempts"); do
        if $COMPOSE_CMD exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list >/dev/null 2>&1; then
            echo "Kafka pronto."
            return 0
        fi

        echo "Tentativo $attempt/$max_attempts: Kafka non ancora pronto..."
        sleep "$sleep_seconds"
    done

    echo "Errore: Kafka non pronto dopo $((max_attempts * sleep_seconds)) secondi."
    return 1
}

create_kafka_topic() {
    echo "Creo il topic Kafka se non esiste: $KAFKA_TOPIC"

    $COMPOSE_CMD exec -T kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server kafka:9092 \
        --create \
        --if-not-exists \
        --topic "$KAFKA_TOPIC" \
        --partitions "$KAFKA_PARTITIONS" \
        --replication-factor 1
}

wait_for_kafka
create_kafka_topic

wait_for_hdfs_file "/nifi_output/merge.csv"

echo "[4/6] Avvio del Job Flink..."
$COMPOSE_CMD run --rm flink-job bash -c "sleep 10 && flink run -d -m jobmanager:8081 -c it.uniroma2.sabd.flink.MainJob /opt/flink/usrlib/analisi-voli-1.0.0.jar --brokers kafka:9092 --topic $KAFKA_TOPIC --parallelism $KAFKA_PARTITIONS"
echo "Job Flink sottomesso al cluster."

echo ""

echo "[5/6] Avvio del Replay Engine..."
$COMPOSE_CMD run --rm replay
echo "Replay Engine completato."

echo ""

echo "[6/6] Indirizzi dei servizi esposti:"
echo " - Kafka UI:      http://localhost:8080"
echo " - Hadoop (HDFS): http://localhost:9870"
echo " - Apache NiFi:   https://localhost:8443/nifi/login"
echo " - Apache Flink:  http://localhost:8081"
echo ""

echo "Infrastruttura avviata con successo!"
