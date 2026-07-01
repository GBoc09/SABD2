package it.uniroma2.sabd.config;

import it.uniroma2.sabd.flink.engineering.watermarks.BoundedOutOfOrderStrategy;
import it.uniroma2.sabd.flink.engineering.watermarks.AdaptiveStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import java.io.InputStream;
import java.util.Properties;
import it.uniroma2.sabd.flink.engineering.watermarks.WatermarkFactory;

public class AppConfig {

    private final String kafkaBootstrapServers;
    private final String kafkaTopic;
    private final int    flinkParallelism;

    // Watermark
    private final long watermarkMaxOutOfOrderMs;
    // Metrics
    private final long metricsThroughputIntervalMs;
    private final long metricsLatencyIntervalMs;
    private final String performanceOutputPath;
    private final long oooReportEveryEvents;
    Properties props = new Properties();
    private AppConfig(ParameterTool params) {

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

        this.performanceOutputPath = params.get("performance-output",
                System.getenv().getOrDefault("PERFORMANCE_OUTPUT_PATH",
                        props.getProperty("metrics.performance.output.path", "performance")));

        this.oooReportEveryEvents = Long.parseLong(
                props.getProperty("metrics.ooo.report.every.events", "1000"));
    }
    public WatermarkFactory getWatermarkStrategy() {
        String strategy = props.getProperty("flink.watermark.strategy", "bounded");
        switch (strategy) {
            case "adaptive": return new AdaptiveStrategy();
            case "bounded":
            default:          return new BoundedOutOfOrderStrategy(watermarkMaxOutOfOrderMs);
        }
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
    public String getPerformanceOutputPath()       { return performanceOutputPath; }
    public long   getOooReportEveryEvents()       { return oooReportEveryEvents; }
}
