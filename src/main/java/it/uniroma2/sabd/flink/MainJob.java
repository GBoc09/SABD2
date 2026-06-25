package it.uniroma2.sabd.flink;

import it.uniroma2.sabd.config.AppConfig;
import it.uniroma2.sabd.flink.controller.FlightQueryController;
import it.uniroma2.sabd.flink.controller.LatencyMonitor;
import it.uniroma2.sabd.flink.controller.PerformanceMetricTags;
import it.uniroma2.sabd.flink.controller.ThroughputMonitor;
import it.uniroma2.sabd.flink.io.kafka.FlightEventDeserializer;
import it.uniroma2.sabd.flink.io.kafka.FlightKafkaSourceFactory;
import it.uniroma2.sabd.flink.io.sink.PerformanceSinks;
import it.uniroma2.sabd.model.FlightEvent;
import it.uniroma2.sabd.flink.engineering.watermarks.WatermarkType;
import it.uniroma2.sabd.flink.engineering.watermarks.WatermarkRegistry;

import it.uniroma2.sabd.flink.controller.OutOfOrderDetector;
import it.uniroma2.sabd.flink.model.OutOfOrderEvent;
import it.uniroma2.sabd.flink.controller.OutOfOrderStatisticProcessor;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class MainJob {

    public static void main(String[] args) throws Exception {

        AppConfig config = AppConfig.fromArgs(args);
        printStartupConfig(config);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.getFlinkParallelism());

        DataStream<FlightEvent> rawStream = buildRawFlightStream(env, config);

        for (WatermarkType type : WatermarkType.values()) {
                DataStream<FlightEvent> wmStream =
                        rawStream.assignTimestampsAndWatermarks(
                                WatermarkRegistry
                                .get(type, config)
                                .create());
                FlightQueryController controller = new FlightQueryController(config, type.name());
                controller.buildQueries(wmStream);
                System.out.println("Avvio query con watermark: "+ type.name());

        }
            
        env.execute("Flight Out-Of-Order Monitor");
    }

    private static DataStream<FlightEvent> buildRawFlightStream(
            StreamExecutionEnvironment env,
            AppConfig config) {

        DataStream<String> kafkaStream = createKafkaStream(env, config);

        DataStream<FlightEvent> deserializedStream = kafkaStream
                .map(new FlightEventDeserializer())
                .name("Deserialize Flight Events");

        SingleOutputStreamOperator<FlightEvent> latencyMonitoredStream = deserializedStream
                .process(new LatencyMonitor<>("producer->flink",
                        config.getMetricsLatencyIntervalMs()))
                .name("Latency Monitor");

        SingleOutputStreamOperator<FlightEvent> monitoredStream = latencyMonitoredStream
                .process(new ThroughputMonitor<>("ingresso",
                        config.getMetricsThroughputIntervalMs()))
                .name("Throughput Monitor");

        PerformanceSinks.writeLatencyCsv(
                latencyMonitoredStream.getSideOutput(PerformanceMetricTags.LATENCY),
                config.getPerformanceOutputPath());
        PerformanceSinks.writeThroughputCsv(
                monitoredStream.getSideOutput(PerformanceMetricTags.THROUGHPUT),
                config.getPerformanceOutputPath());

        DataStream<OutOfOrderEvent> outOfOrderStream =
        monitoredStream
                .keyBy(event -> "GLOBAL")
                .process(new OutOfOrderDetector());

        outOfOrderStream
                .keyBy(event -> "GLOBAL")
                .process(
                new OutOfOrderStatisticProcessor(
                        config.getOooReportEveryEvents()))
                .name("Out Of Order Statistics");


        return monitoredStream;                                              
                /*.assignTimestampsAndWatermarks(createEventTimeAssigner(config))
                .name("Assign Event Time");*/
    }

    private static DataStream<String> createKafkaStream(
            StreamExecutionEnvironment env, AppConfig config) {

        KafkaSource<String> source = FlightKafkaSourceFactory.create(config);

        return env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source");
    }
    
    private static WatermarkStrategy<FlightEvent> createEventTimeAssigner(AppConfig config) {
        return config.getWatermarkStrategy().create();
    }

    private static void printStartupConfig(AppConfig config) {
        System.out.println("Avvio Flink Job con i seguenti parametri:");
        System.out.println(" - Kafka Brokers: " + config.getKafkaBootstrapServers());
        System.out.println(" - Kafka Topic:   " + config.getKafkaTopic());
        System.out.println(" - Parallelismo:  " + config.getFlinkParallelism());
        System.out.println(" - Performance:   " + config.getPerformanceOutputPath());
    }
}
