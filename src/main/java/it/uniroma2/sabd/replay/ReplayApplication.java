package it.uniroma2.sabd.replay;

import it.uniroma2.sabd.model.FlightEvent;
import it.uniroma2.sabd.replay.loader.HdfsFlightLoader;
import it.uniroma2.sabd.kafka.KafkaTopicManager;
import it.uniroma2.sabd.kafka.KafkaFlightProducer;
import it.uniroma2.sabd.config.ReplayConfig;

import java.util.List;

public class ReplayApplication {
    public static void main(String[] args) throws Exception {

        ReplayConfig config = new ReplayConfig();

        System.out.println("--- Avvio Replay Engine ---");
        System.out.println("Reading HDFS file " + config.getHdfsFilePath() + " from " + config.getHdfsUri());
        System.out.println("Kafka broker: " + config.getKafkaBootstrapServers());
        System.out.println("Kafka topic: " + config.getKafkaTopic() + " (Partitions: " + config.getKafkaPartitions() + ")");
        System.out.println("Acceleration factor: " + config.getAccelerationFactor() + "x");
        System.out.println("---------------------------");

        KafkaTopicManager topicManager = new KafkaTopicManager(config.getKafkaBootstrapServers());

        topicManager.createTopicIfNotExists(
                config.getKafkaTopic(),
                config.getKafkaPartitions(),
                (short) 1
        );

        KafkaFlightProducer producer = new KafkaFlightProducer(
                config.getKafkaBootstrapServers(),
                config.getKafkaTopic()
        );

        //Caricamento Dati
        HdfsFlightLoader loader = new HdfsFlightLoader();
        List<FlightEvent> events = loader.load(
                config.getHdfsUri(),
                config.getHdfsFilePath()
        );

        System.out.println("Loaded events: " + events.size());

        //Avvio Replay
        ReplayEngine replayEngine = new ReplayEngine(
                config.getAccelerationFactor(),
                producer
        );

        replayEngine.replay(events);

        producer.close();
    }
}