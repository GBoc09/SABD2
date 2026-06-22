package it.uniroma2.sabd.flink;

import it.uniroma2.sabd.config.AppConfig;
import it.uniroma2.sabd.model.FlightEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

//import it.uniroma2.sabd.flink.watermark.FlightWatermarkStrategy;    

import java.time.ZoneOffset;

public class FlinkKafkaConsumerJob {

    public static void main(String[] args) throws Exception {

        // 1. CONFIGURAZIONE
        AppConfig config = AppConfig.fromArgs(args);
        System.out.println("Avvio Flink Job con i seguenti parametri:");
        System.out.println(" - Kafka Brokers: " + config.getKafkaBootstrapServers());
        System.out.println(" - Kafka Topic:   " + config.getKafkaTopic());
        System.out.println(" - Parallelismo:  " + config.getFlinkParallelism());

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.getFlinkParallelism());

        // 2. SORGENTE KAFKA
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

        // 3. DESERIALIZZAZIONE JSON → FlightEvent
        DataStream<FlightEvent> flightStream = stringStream.map(
                new MapFunction<String, FlightEvent>() {
                    private transient ObjectMapper mapper;

                    @Override
                    public FlightEvent map(String json) throws Exception {
                        if (mapper == null) {
                            mapper = new ObjectMapper();
                            mapper.registerModule(new JavaTimeModule());
                        }
                        return mapper.readValue(json, FlightEvent.class);
                    }
                }
        );//.flightStream.assignTimestampsAndWatermarks(
        //FlightWatermarkStrategy.create()
        //);

        // 4. RILEVAMENTO FUORI ORDINE
        //
        // keyBy(carrier): eventi dello stesso carrier vanno sempre allo stesso
        // task → il confronto temporale è coerente e lo stato è per-chiave.
        //
        // KeyedProcessFunction + ValueState<Long>: il "massimo timestamp visto"
        // è mantenuto da Flink per ogni carrier separatamente, in modo
        // fault-tolerant e distribuito correttamente.
        flightStream
                .keyBy(FlightEvent::getCarrier)
                .process(new KeyedProcessFunction<String, FlightEvent, FlightEvent>() {

                    // Stato Flink: timestamp massimo visto per questo carrier (epoch ms)
                    private ValueState<Long> maxTimestampState;

                    @Override
                    public void open(Configuration parameters) {
                        maxTimestampState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("maxTimestamp", Long.class)
                        );
                    }

                    @Override
                    public void processElement(
                            FlightEvent event,
                            Context ctx,
                            Collector<FlightEvent> out) throws Exception {

                        if (event.getEventTime() == null) {
                            out.collect(event);
                            return;
                        }

                        long currentMs = event.getEventTime()
                                .toEpochSecond(ZoneOffset.UTC) * 1000L;

                        Long maxMs = maxTimestampState.value();

                        if (maxMs == null) {
                            // primo evento per questo carrier
                            maxTimestampState.update(currentMs);
                        } else if (currentMs < maxMs) {
                            // evento arrivato fuori ordine
                            long delayMs = maxMs - currentMs;
                            System.out.println(
                                    "⚠️  FUORI ORDINE!"
                                            + " Carrier: "   + event.getCarrier()
                                            + " | Partenza: " + event.getEventTime()
                                            + " | Ritardo logico: " + delayMs + " ms"
                            );
                        } else {
                            // evento in ordine: aggiorna il massimo
                            maxTimestampState.update(currentMs);
                        }

                        out.collect(event);
                    }
                })
                .print(); // sostituisci con sink reale quando necessario

        env.execute("Flight Out-Of-Order Monitor");
    }
}