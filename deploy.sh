#!/bin/bash
set -e

COMPOSE_CMD="sudo docker compose --env-file docker/.env -f docker/docker-compose.yml"
KAFKA_TOPIC="flights"
KAFKA_PARTITIONS="2"
FLINK_PARALLELISM="2"
FLINK_WATERMARK=""

usage() {
    echo "Uso: $0 --watermark WM15|WM100|ADAPTIVE [--parallelism N]"
    echo ""
    echo "Esempi:"
    echo "  $0 --watermark WM15"
    echo "  $0 --watermark WM100 --parallelism 2"
    echo "  $0 --watermark ADAPTIVE --parallelism 4"
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
    usage
    exit 0
fi

if [ "$#" -lt 2 ] || [ "$1" != "--watermark" ]; then
    echo "Errore: devi specificare il watermark da eseguire."
    usage
    exit 1
fi

FLINK_WATERMARK="${2^^}"
shift 2

if [ "$#" -gt 0 ]; then
    if [ "$#" -ne 2 ] || [ "$1" != "--parallelism" ]; then
        echo "Errore: l'unico parametro opzionale ammesso e' --parallelism N."
        usage
        exit 1
    fi

    FLINK_PARALLELISM="$2"
fi

case "$FLINK_WATERMARK" in
    WM15|WM100|ADAPTIVE)
        ;;
    "")
        echo "Errore: devi specificare un watermark."
        usage
        exit 1
        ;;
    *)
        echo "Errore: watermark non valido: $FLINK_WATERMARK"
        usage
        exit 1
        ;;
esac

case "$FLINK_PARALLELISM" in
    ""|*[!0-9]*)
        echo "Errore: --parallelism deve essere un intero positivo."
        usage
        exit 1
        ;;
esac

if [ "$FLINK_PARALLELISM" -lt 1 ]; then
    echo "Errore: --parallelism deve essere almeno 1."
    usage
    exit 1
fi

echo "=============================================="
echo "   Avvio Pipeline Streaming - SABD2           "
echo "=============================================="
echo ""
echo "Configurazione run:"
echo " - Watermark:    $FLINK_WATERMARK"
echo " - Kafka topic:  $KAFKA_TOPIC"
echo " - Parallelismo: $FLINK_PARALLELISM"
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

ensure_no_running_flink_jobs() {
    echo "Controllo che non ci siano job Flink gia' attivi..."

    local running_jobs
    running_jobs=$($COMPOSE_CMD exec -T jobmanager flink list 2>/dev/null | grep RUNNING | awk '{print $4}' || true)

    if [ -n "$running_jobs" ]; then
        echo "Errore: esiste gia' almeno un job Flink RUNNING:"
        echo "$running_jobs"
        echo "Esporta i risultati, ferma il job attivo e poi rilancia deploy.sh con il watermark successivo."
        exit 1
    fi
}

wait_for_kafka
create_kafka_topic

wait_for_hdfs_file "/nifi_output/merge.csv"

echo "[4/6] Avvio del Job Flink..."
ensure_no_running_flink_jobs
$COMPOSE_CMD run --rm flink-job bash -c "sleep 10 && flink run -d -m jobmanager:8081 -c it.uniroma2.sabd.flink.MainJob /opt/flink/usrlib/analisi-voli-1.0.0.jar --brokers kafka:9092 --topic $KAFKA_TOPIC --parallelism $FLINK_PARALLELISM --watermark $FLINK_WATERMARK"
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
