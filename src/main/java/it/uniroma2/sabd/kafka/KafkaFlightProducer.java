package it.uniroma2.sabd.kafka;

import it.uniroma2.sabd.model.FlightEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaFlightProducer implements AutoCloseable {

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final Integer partition;
    private final ObjectMapper mapper;

    public KafkaFlightProducer(String bootstrapServers, String topic) {
        this(bootstrapServers, topic, null, null);
    }

    public KafkaFlightProducer(String bootstrapServers, String topic, Integer partition, String clientId) {
        this.topic = topic;
        this.partition = partition;

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        if (clientId != null && !clientId.isBlank()) {
            props.put("client.id", clientId);
        }

        this.producer = new KafkaProducer<>(props);

        this.mapper = new ObjectMapper();
        this.mapper.registerModule(new JavaTimeModule());
    }

    public void send(FlightEvent event) {
        try {
            event.setProducedAt(System.currentTimeMillis());
            String json = mapper.writeValueAsString(event);
            producer.send(new ProducerRecord<>(topic, partition, null, json));
        } catch (Exception e) {
            System.err.println("Error sending event to Kafka: " + e.getMessage());
            throw new RuntimeException("Failed to send event to Kafka", e);
        }
    }

    public void flush() {
        producer.flush();
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.close();
        }
    }
}
