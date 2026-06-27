package it.uniroma2.sabd.config;

import java.io.InputStream;
import java.util.Properties;

/**
 * Classe responsabile ESCLUSIVAMENTE di fornire i parametri al Replay Engine.
 * Legge da application.properties o dalle variabili d'ambiente di Docker.
 */
public class ReplayConfig {

    private final String hdfsUri;
    private final String hdfsFilePath;
    private final String kafkaBootstrapServers;
    private final String kafkaTopic;
    private final int kafkaPartitions;
    private final long accelerationFactor;
    private final int producerCount;
    private final int replayKafkaPartition;
    private final long maxNetworkDelayMillis;
    private final int speedSkewPercent;
    private final long randomSeed;

    public ReplayConfig() {
        Properties props = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            if (input != null) {
                props.load(input);
            }
        } catch (Exception e) {
            System.err.println("Impossibile caricare application.properties, uso i valori di default.");
        }

        this.hdfsUri = System.getenv().getOrDefault("HDFS_URI", props.getProperty("hdfs.uri", "hdfs://namenode:8020"));
        this.hdfsFilePath = System.getenv().getOrDefault("HDFS_FILE_PATH", props.getProperty("hdfs.file.path", "/nifi_output/merge.csv"));

        this.kafkaBootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", props.getProperty("kafka.bootstrap.servers", "kafka:9092"));
        this.kafkaTopic = System.getenv().getOrDefault("KAFKA_TOPIC", props.getProperty("kafka.topic", "flights"));
        this.kafkaPartitions = Integer.parseInt(System.getenv().getOrDefault("KAFKA_PARTITIONS", props.getProperty("kafka.partitions", "4")));

        this.accelerationFactor = Long.parseLong(System.getenv().getOrDefault("REPLAY_ACCELERATION_FACTOR", props.getProperty("replay.acceleration.factor", "20000")));
        this.producerCount = Integer.parseInt(System.getenv().getOrDefault("REPLAY_PRODUCER_COUNT", props.getProperty("replay.producer.count", "4")));
        this.replayKafkaPartition = Integer.parseInt(System.getenv().getOrDefault("REPLAY_KAFKA_PARTITION", props.getProperty("replay.kafka.partition", "0")));
        this.maxNetworkDelayMillis = Long.parseLong(System.getenv().getOrDefault("REPLAY_MAX_NETWORK_DELAY_MS", props.getProperty("replay.max.network.delay.ms", "250")));
        this.speedSkewPercent = Integer.parseInt(System.getenv().getOrDefault("REPLAY_SPEED_SKEW_PERCENT", props.getProperty("replay.speed.skew.percent", "15")));
        this.randomSeed = Long.parseLong(System.getenv().getOrDefault("REPLAY_RANDOM_SEED", props.getProperty("replay.random.seed", "42")));

        validate();
    }

    private void validate() {
        if (kafkaPartitions < 1) {
            throw new IllegalArgumentException("kafka.partitions deve essere almeno 1");
        }
        if (accelerationFactor < 1) {
            throw new IllegalArgumentException("replay.acceleration.factor deve essere almeno 1");
        }
        if (producerCount < 1) {
            throw new IllegalArgumentException("replay.producer.count deve essere almeno 1");
        }
        if (replayKafkaPartition < 0 || replayKafkaPartition >= kafkaPartitions) {
            throw new IllegalArgumentException("replay.kafka.partition deve essere compresa tra 0 e kafka.partitions - 1");
        }
        if (maxNetworkDelayMillis < 0) {
            throw new IllegalArgumentException("replay.max.network.delay.ms non puo' essere negativo");
        }
        if (speedSkewPercent < 0) {
            throw new IllegalArgumentException("replay.speed.skew.percent non puo' essere negativo");
        }
    }

    // --- Getters ---
    public String getHdfsUri() {
        return hdfsUri;
    }

    public String getHdfsFilePath() {
        return hdfsFilePath;
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public int getKafkaPartitions() {
        return kafkaPartitions;
    }

    public long getAccelerationFactor() {
        return accelerationFactor;
    }

    public int getProducerCount() {
        return producerCount;
    }

    public int getReplayKafkaPartition() {
        return replayKafkaPartition;
    }

    public long getMaxNetworkDelayMillis() {
        return maxNetworkDelayMillis;
    }

    public int getSpeedSkewPercent() {
        return speedSkewPercent;
    }

    public long getRandomSeed() {
        return randomSeed;
    }
}
