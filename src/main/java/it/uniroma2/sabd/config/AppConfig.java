package it.uniroma2.sabd.config;

import org.apache.flink.api.java.utils.ParameterTool;
import java.io.InputStream;
import java.util.Properties;

public class AppConfig {

    private final String kafkaBootstrapServers;
    private final String kafkaTopic;
    private final int    flinkParallelism;

    // Watermark
    private final long watermarkMaxOutOfOrderMs;

    // Metrics
    private final long metricsThroughputIntervalMs;
    private final long metricsLatencyIntervalMs;
    private final long oooReportEveryEvents;

    private AppConfig(ParameterTool params) {
        Properties props = new Properties();
        try (InputStream input = AppConfig.class
                .getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (input != null) props.load(input);
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

        this.watermarkMaxOutOfOrderMs = Long.parseLong(
                System.getenv().getOrDefault("FLINK_WATERMARK_MS",
                        props.getProperty("flink.watermark.max.out.of.order.ms", "600000")));

        this.metricsThroughputIntervalMs = Long.parseLong(
                props.getProperty("metrics.throughput.report.interval.ms", "5000"));

        this.metricsLatencyIntervalMs = Long.parseLong(
                props.getProperty("metrics.latency.report.interval.ms", "5000"));

        this.oooReportEveryEvents = Long.parseLong(
                props.getProperty("metrics.ooo.report.every.events", "1000"));
    }

    public static AppConfig fromArgs(String[] args) {
        return new AppConfig(ParameterTool.fromArgs(args));
    }

    public String getKafkaBootstrapServers()      { return kafkaBootstrapServers; }
    public String getKafkaTopic()                 { return kafkaTopic; }
    public int    getFlinkParallelism()           { return flinkParallelism; }
    public long   getWatermarkMaxOutOfOrderMs()   { return watermarkMaxOutOfOrderMs; }
    public long   getMetricsThroughputIntervalMs(){ return metricsThroughputIntervalMs; }
    public long   getMetricsLatencyIntervalMs()   { return metricsLatencyIntervalMs; }
    public long   getOooReportEveryEvents()       { return oooReportEveryEvents; }
}