package it.uniroma2.sabd.config;

import org.apache.flink.api.java.utils.ParameterTool;
import java.io.InputStream;
import java.util.Properties;

public class AppConfig {

    private final String kafkaBootstrapServers;
    private final String kafkaTopic;
    private final int flinkParallelism;

    private AppConfig(ParameterTool params) {
        Properties props = new Properties();
        try (InputStream input = AppConfig.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input != null) {
                props.load(input);
            }
        } catch (Exception e) {
            System.err.println("Impossibile caricare application.properties per Flink.");
        }

        this.kafkaBootstrapServers = params.get("brokers",
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS",
                        props.getProperty("kafka.bootstrap.servers", "kafka:9092")));

        this.kafkaTopic = params.get("topic",
                System.getenv().getOrDefault("KAFKA_TOPIC",
                        props.getProperty("kafka.topic", "flights")));

        this.flinkParallelism = params.getInt("parallelism",
                Integer.parseInt(System.getenv().getOrDefault("FLINK_PARALLELISM",
                        props.getProperty("flink.parallelism", "4"))));
    }

    // Metodo Factory per costruire la configurazione
    public static AppConfig fromArgs(String[] args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        return new AppConfig(params);
    }

    public String getKafkaBootstrapServers() { return kafkaBootstrapServers; }
    public String getKafkaTopic() { return kafkaTopic; }
    public int getFlinkParallelism() { return flinkParallelism; }
}