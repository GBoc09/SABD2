package it.uniroma2.sabd.config;


import org.apache.flink.api.java.utils.ParameterTool;

public class AppConfig {

    private final String kafkaBootstrapServers;
    private final String kafkaTopic;
    private final int flinkParallelism;

    private AppConfig(ParameterTool params) {
        // 1. Cerca nei parametri passati da riga di comando (es. --brokers kafka:9092)
        // 2. Se non c'è, cerca nelle variabili d'ambiente
        // 3. Se non c'è, usa il valore di default
        this.kafkaBootstrapServers = params.get("brokers",
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"));

        this.kafkaTopic = params.get("topic",
                System.getenv().getOrDefault("KAFKA_TOPIC", "flights"));

        this.flinkParallelism = params.getInt("parallelism",
                Integer.parseInt(System.getenv().getOrDefault("FLINK_PARALLELISM", "4")));
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