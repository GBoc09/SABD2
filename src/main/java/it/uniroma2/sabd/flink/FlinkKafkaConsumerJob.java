package it.uniroma2.sabd.flink;

import it.uniroma2.sabd.config.AppConfig;
import it.uniroma2.sabd.model.FlightEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.LocalDateTime;

public class FlinkKafkaConsumerJob {

    public static void main(String[] args) throws Exception {

        // 1. INIEZIONE DELLA CONFIGURAZIONE (Portabilità!)
        AppConfig config = AppConfig.fromArgs(args);

        System.out.println("Avvio Flink Job con i seguenti parametri:");
        System.out.println(" - Kafka Brokers: " + config.getKafkaBootstrapServers());
        System.out.println(" - Kafka Topic:   " + config.getKafkaTopic());
        System.out.println(" - Parallelismo:  " + config.getFlinkParallelism());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.getFlinkParallelism());

        // 2. CONFIGURAZIONE SORGENTE KAFKA
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(config.getKafkaBootstrapServers())
                .setTopics(config.getKafkaTopic())
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> stringStream = env.fromSource(
                source,
                org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        // 3. DESERIALIZZAZIONE
        DataStream<FlightEvent> flightStream = stringStream.map(new MapFunction<String, FlightEvent>() {
            private transient ObjectMapper mapper;

            @Override
            public FlightEvent map(String json) throws Exception {
                if (mapper == null) {
                    mapper = new ObjectMapper();
                    mapper.registerModule(new JavaTimeModule());
                }
                return mapper.readValue(json, FlightEvent.class);
            }
        });

        // 4. LOGICA DI BUSINESS (Trappola Fuori Ordine)
        flightStream.process(new ProcessFunction<FlightEvent, FlightEvent>() {
            private transient LocalDateTime maxTimeSeen;

            @Override
            public void processElement(FlightEvent event, Context ctx, Collector<FlightEvent> out) {
                LocalDateTime currentEventTime = event.getEventTime();

                if (currentEventTime == null) return;

                if (maxTimeSeen == null) {
                    maxTimeSeen = currentEventTime;
                } else if (currentEventTime.isBefore(maxTimeSeen)) {
                    long delayMs = Duration.between(currentEventTime, maxTimeSeen).toMillis();
                    System.out.println("⚠️ FUORI ORDINE! Volo: " + event.getCarrier() +
                            " | Partenza: " + currentEventTime +
                            " | Ritardo logico: " + delayMs + " ms");
                } else {
                    maxTimeSeen = currentEventTime;
                }
                out.collect(event);
            }
        });

        env.execute("Flight Out-Of-Order Monitor");
    }
}