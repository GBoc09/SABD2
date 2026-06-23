package it.uniroma2.sabd.flink;

import it.uniroma2.sabd.config.AppConfig;
import it.uniroma2.sabd.flink.metrics.LatencyMonitor;
import it.uniroma2.sabd.flink.metrics.ThroughputMonitor;
import it.uniroma2.sabd.model.FlightEvent;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import it.uniroma2.sabd.flink.serialization.FlightEventDeserializer;
import it.uniroma2.sabd.flink.process.OutOfOrderDetector;
import it.uniroma2.sabd.flink.source.FlightKafkaSourceFactory;


public class FlinkKafkaConsumerJob {

    public static void main(String[] args) throws Exception {

        AppConfig config = AppConfig.fromArgs(args);
        System.out.println("Avvio Flink Job con i seguenti parametri:");
        System.out.println(" - Kafka Brokers: " + config.getKafkaBootstrapServers());
        System.out.println(" - Kafka Topic:   " + config.getKafkaTopic());
        System.out.println(" - Parallelismo:  " + config.getFlinkParallelism());

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.getFlinkParallelism());

        KafkaSource<String> source = FlightKafkaSourceFactory.create(config);

        DataStream<String> stringStream = env.fromSource(
                source,
                org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );
        
        DataStream<FlightEvent> flightStream =
                stringStream.map(new FlightEventDeserializer());

        DataStream<FlightEvent> monitoredStream = flightStream
                .process(new LatencyMonitor<>("kafka→flink",
                        config.getMetricsLatencyIntervalMs()));


        monitoredStream = monitoredStream
                .process(new ThroughputMonitor<>("ingresso",
                        config.getMetricsThroughputIntervalMs()));

        monitoredStream
                .keyBy(event -> "GLOBAL")
                .process(new OutOfOrderDetector(config.getOooReportEveryEvents()))
                .print();

        env.execute("Flight Out-Of-Order Monitor");
    }
}