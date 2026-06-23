package it.uniroma2.sabd.flink;

import it.uniroma2.sabd.config.AppConfig;
import it.uniroma2.sabd.flink.controller.FlightQueryController;
import it.uniroma2.sabd.flink.metrics.LatencyMonitor;
import it.uniroma2.sabd.flink.metrics.ThroughputMonitor;
import it.uniroma2.sabd.flink.serialization.FlightEventDeserializer;
import it.uniroma2.sabd.flink.source.FlightKafkaSourceFactory;
import it.uniroma2.sabd.model.FlightEvent;
import java.time.Duration;
import java.time.ZoneOffset;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class MainJob {

    public static void main(String[] args) throws Exception {

        AppConfig config = AppConfig.fromArgs(args);
        printStartupConfig(config);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.getFlinkParallelism());

        DataStream<FlightEvent> flightStream = buildFlightStream(env, config);

        FlightQueryController controller = new FlightQueryController(config);
        controller.buildQueries(flightStream);

        env.execute("Flight Out-Of-Order Monitor");
    }

    private static DataStream<FlightEvent> buildFlightStream(
            StreamExecutionEnvironment env,
            AppConfig config) {

        DataStream<String> kafkaStream = createKafkaStream(env, config);

        DataStream<FlightEvent> deserializedStream = kafkaStream
                .map(new FlightEventDeserializer())
                .name("Deserialize Flight Events");

        DataStream<FlightEvent> monitoredStream = deserializedStream
                .process(new LatencyMonitor<>("kafka->flink",
                        config.getMetricsLatencyIntervalMs()))
                .name("Latency Monitor")
                .process(new ThroughputMonitor<>("ingresso",
                        config.getMetricsThroughputIntervalMs()))
                .name("Throughput Monitor");

        return monitoredStream                                               
                .assignTimestampsAndWatermarks(createEventTimeAssigner(config))
                .name("Assign Event Time");
    }

    private static DataStream<String> createKafkaStream(
            StreamExecutionEnvironment env,
            AppConfig config) {

        KafkaSource<String> source = FlightKafkaSourceFactory.create(config);

        return env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source");
    }
    
    private static WatermarkStrategy<FlightEvent> createEventTimeAssigner(AppConfig config) {
        return WatermarkStrategy
                .<FlightEvent>forBoundedOutOfOrderness(
                        Duration.ofMillis(config.getWatermarkMaxOutOfOrderMs()))
                .withIdleness(Duration.ofSeconds(30))
                .withTimestampAssigner((event, previousTimestamp) ->           
                        event.getEventTime()
                                .toInstant(ZoneOffset.UTC)
                                .toEpochMilli());
    }

    private static void printStartupConfig(AppConfig config) {
        System.out.println("Avvio Flink Job con i seguenti parametri:");
        System.out.println(" - Kafka Brokers: " + config.getKafkaBootstrapServers());
        System.out.println(" - Kafka Topic:   " + config.getKafkaTopic());
        System.out.println(" - Parallelismo:  " + config.getFlinkParallelism());
    }
}
