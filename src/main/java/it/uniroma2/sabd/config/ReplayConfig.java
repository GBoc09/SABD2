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
}