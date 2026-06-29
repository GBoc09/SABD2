package it.uniroma2.sabd.flink;

import it.uniroma2.sabd.config.AppConfig;
import it.uniroma2.sabd.flink.controller.FlightQueryController;
import it.uniroma2.sabd.flink.controller.WatermarkLateEventDetector;
import it.uniroma2.sabd.flink.io.kafka.FlightEventDeserializer;
import it.uniroma2.sabd.flink.io.kafka.FlightKafkaSourceFactory;
import it.uniroma2.sabd.flink.io.sink.QuerySinks;
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

        for (WatermarkType watermarkType : WatermarkType.values()) {
            buildWatermarkPipeline(rawStream, config, watermarkType);
        }
            
        env.execute("Flight Watermark Strategy Comparison");
    }

    private static void buildWatermarkPipeline(
            DataStream<FlightEvent> rawStream,
            AppConfig config,
            WatermarkType watermarkType) {

        String watermarkName = watermarkType.name();

        DataStream<FlightEvent> wmStream =
                rawStream.assignTimestampsAndWatermarks(
                        WatermarkRegistry
                                .get(watermarkType, config)
                                .create())
                        .name("Assign Event Time " + watermarkName);

        SingleOutputStreamOperator<FlightEvent> watermarkCheckedStream = wmStream
                .process(new WatermarkLateEventDetector())
                .name("Watermark Late Event Detector " + watermarkName);

        watermarkCheckedStream
                .getSideOutput(WatermarkLateEventDetector.LATE_AFTER_WATERMARK_TAG)
                .sinkTo(QuerySinks.watermarkLateEventsCsv(watermarkName))
                .name("Watermark Late Events CSV Sink " + watermarkName);

        FlightQueryController controller = new FlightQueryController(config, watermarkName);
        controller.buildQueries(watermarkCheckedStream);
        System.out.println("Avvio query con watermark: " + watermarkName);
    }

    private static DataStream<FlightEvent> buildRawFlightStream(
            StreamExecutionEnvironment env,
            AppConfig config) {

        DataStream<String> kafkaStream = createKafkaStream(env, config);

        DataStream<FlightEvent> deserializedStream = kafkaStream
                .map(new FlightEventDeserializer())
                .name("Deserialize Flight Events");

        DataStream<OutOfOrderEvent> outOfOrderStream =
        deserializedStream
                .keyBy(event -> "GLOBAL")
                .process(new OutOfOrderDetector());

        outOfOrderStream
                .keyBy(event -> "GLOBAL")
                .process(
                new OutOfOrderStatisticProcessor(
                        config.getOooReportEveryEvents()))
                .name("Out Of Order Statistics");


        return deserializedStream;
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
    
    private static void printStartupConfig(AppConfig config) {
        System.out.println("Avvio Flink Job con i seguenti parametri:");
        System.out.println(" - Kafka Brokers: " + config.getKafkaBootstrapServers());
        System.out.println(" - Kafka Topic:   " + config.getKafkaTopic());
        System.out.println(" - Parallelismo:  " + config.getFlinkParallelism());
        System.out.println(" - Watermark:     WM15, WM100, ADAPTIVE");
        System.out.println(" - Performance:   " + config.getPerformanceOutputPath());
    }
}
