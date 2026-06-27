package it.uniroma2.sabd.replay;

import it.uniroma2.sabd.model.FlightEvent;
import it.uniroma2.sabd.replay.loader.HdfsFlightLoader;
import it.uniroma2.sabd.kafka.KafkaTopicManager;
import it.uniroma2.sabd.kafka.KafkaFlightProducer;
import it.uniroma2.sabd.config.ReplayConfig;

import java.util.ArrayList;
import java.util.List;

public class ReplayApplication {
    public static void main(String[] args) throws Exception {

        ReplayConfig config = new ReplayConfig();

        System.out.println("--- Avvio Replay Engine ---");
        System.out.println("Reading HDFS file " + config.getHdfsFilePath() + " from " + config.getHdfsUri());
        System.out.println("Kafka broker: " + config.getKafkaBootstrapServers());
        System.out.println("Kafka topic: " + config.getKafkaTopic() + " (Partitions: " + config.getKafkaPartitions() + ")");
        System.out.println("Acceleration factor: " + config.getAccelerationFactor() + "x");
        System.out.println("Replay producers: " + config.getProducerCount());
        System.out.println("Replay Kafka partition: " + config.getReplayKafkaPartition());
        System.out.println("Replay max initial network delay: " + config.getMaxNetworkDelayMillis() + " ms");
        System.out.println("Replay speed skew: +/-" + config.getSpeedSkewPercent() + "%");
        System.out.println("Replay random seed: " + config.getRandomSeed());
        System.out.println("---------------------------");

        KafkaTopicManager topicManager = new KafkaTopicManager(config.getKafkaBootstrapServers());

        topicManager.createTopicIfNotExists(
                config.getKafkaTopic(),
                config.getKafkaPartitions(),
                (short) 1
        );

        //Caricamento Dati
        HdfsFlightLoader loader = new HdfsFlightLoader();
        List<FlightEvent> events = loader.load(
                config.getHdfsUri(),
                config.getHdfsFilePath()
        );

        System.out.println("Loaded events: " + events.size());

        List<KafkaFlightProducer> producers =
                createProducers(config);

        try {
            //Avvio Replay
            ReplayEngine replayEngine = new ReplayEngine(
                    config.getAccelerationFactor(),
                    producers,
                    config.getMaxNetworkDelayMillis(),
                    config.getSpeedSkewPercent(),
                    config.getRandomSeed()
            );

            replayEngine.replay(events);
        } finally {
            closeProducers(producers);
        }
    }

    private static List<KafkaFlightProducer> createProducers(ReplayConfig config) {
        List<KafkaFlightProducer> producers =
                new ArrayList<>();

        for (int i = 0; i < config.getProducerCount(); i++) {
            producers.add(
                    new KafkaFlightProducer(
                            config.getKafkaBootstrapServers(),
                            config.getKafkaTopic(),
                            config.getReplayKafkaPartition(),
                            "flight-replay-producer-" + i
                    )
            );
        }

        return producers;
    }

    private static void closeProducers(List<KafkaFlightProducer> producers) {
        for (KafkaFlightProducer producer : producers) {
            try {
                producer.close();
            } catch (Exception e) {
                System.err.println("Error closing Kafka producer: " + e.getMessage());
            }
        }
    }
}
