#!/bin/bash
set -e

COMPOSE_CMD="sudo docker compose --env-file docker/.env -f docker/docker-compose.yml"
KAFKA_TOPIC="flights"
KAFKA_PARTITIONS="2"
FLINK_PARALLELISM="2"

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
    echo "Creo il topic Kafka: $KAFKA_TOPIC"

    $COMPOSE_CMD exec -T kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server kafka:9092 \
        --create \
        --if-not-exists \
        --topic "$KAFKA_TOPIC" \
        --partitions "$KAFKA_PARTITIONS" \
        --replication-factor 1
}

ensure_no_running_flink_jobs() {
    echo "Controllo che non ci siano job Flink gia' attivi..."

    local running_jobs
    running_jobs=$($COMPOSE_CMD exec -T jobmanager flink list 2>/dev/null | grep RUNNING | awk '{print $4}' || true)

    if [ -n "$running_jobs" ]; then
        echo "Errore: esiste gia' almeno un job Flink RUNNING:"
        echo "$running_jobs"
        echo "Esporta i risultati, ferma il job attivo e poi rilancia run_flink_job.sh."
        exit 1
    fi
}


echo "=============================================="
echo "   Avvio Job Flink - SABD2                    "
echo "=============================================="
echo ""
echo "Configurazione run:"
echo " - Watermark:    WM15, WM100, ADAPTIVE"
echo " - Kafka topic:  $KAFKA_TOPIC"
echo " - Parallelismo: $FLINK_PARALLELISM"
echo ""

wait_for_kafka
ensure_no_running_flink_jobs
create_kafka_topic

echo "[1/2] Avvio del Job Flink con tutte le strategie watermark..."
$COMPOSE_CMD run --rm flink-job bash -c "sleep 10 && flink run -d -m jobmanager:8081 -c it.uniroma2.sabd.flink.MainJob /opt/flink/usrlib/analisi-voli-1.0.0.jar --brokers kafka:9092 --topic $KAFKA_TOPIC --parallelism $FLINK_PARALLELISM"
echo "Job Flink sottomesso al cluster."

echo ""

echo "[2/2] Avvio del Replay Engine..."
$COMPOSE_CMD run --rm replay
echo "Replay Engine completato."

echo ""
echo "Run completata per tutte le strategie watermark."
