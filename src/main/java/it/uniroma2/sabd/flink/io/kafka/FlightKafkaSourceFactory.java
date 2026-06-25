package it.uniroma2.sabd.flink.io.kafka;

import it.uniroma2.sabd.config.AppConfig;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public final class FlightKafkaSourceFactory {

    private FlightKafkaSourceFactory() {
    }

    public static KafkaSource<String> create(AppConfig config) {

        return KafkaSource.<String>builder()
                .setBootstrapServers(config.getKafkaBootstrapServers())
                .setTopics(config.getKafkaTopic())
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }
}
