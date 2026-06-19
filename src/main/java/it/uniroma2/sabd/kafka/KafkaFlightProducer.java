package it.uniroma2.sabd.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import it.uniroma2.sabd.model.FlightEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaFlightProducer {

    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final ObjectMapper objectMapper;

    public KafkaFlightProducer(
            String bootstrapServers,
            String topic
    ) {

        this.topic = topic;

        Properties props = new Properties();

        props.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers
        );

        props.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName()
        );

        this.producer = new KafkaProducer<>(props);

        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    public void send(FlightEvent event) {

        try {

            String json =
                    objectMapper.writeValueAsString(event);

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(
                            topic,
                            null, 
                            json
                    );

            producer.send(record);

        } catch (Exception e) {
            System.err.println(
                    "Error sending event to Kafka: "
                            + e.getMessage()
            );
        }
    }

    public void close() {
        producer.flush();
        producer.close();
    }
}