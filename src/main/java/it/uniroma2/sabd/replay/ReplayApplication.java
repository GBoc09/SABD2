package it.uniroma2.sabd.replay; 

import it.uniroma2.sabd.model.FlightEvent; 
import it.uniroma2.sabd.replay.loader.HdfsFlightLoader;
import it.uniroma2.sabd.kafka.KafkaTopicManager;
import it.uniroma2.sabd.kafka.KafkaFlightProducer;


import java.util.List;


public class ReplayApplication {
    public static void main(String[] args) throws Exception {
        String hdfsUri =
                System.getenv().getOrDefault(
                                "HDFS_URI",
                                "hdfs://namenode:8020"
        );

        String hdfsFilePath =
                System.getenv()
                        .getOrDefault(
                                "HDFS_FILE_PATH",
                                "/nifi_output/merge.csv"
                        );

        System.out.println(
                "Reading HDFS file "
                        + hdfsFilePath
                        + " from "
                        + hdfsUri
        );

        String bootstrapServers =
            System.getenv()
                .getOrDefault(
                        "KAFKA_BOOTSTRAP_SERVERS",
                        "kafka:9092"
                );

        String topic =
            System.getenv()
                .getOrDefault(
                        "KAFKA_TOPIC",
                        "flights"
            );
        int partitions = Integer.parseInt(
                System.getenv().getOrDefault("KAFKA_PARTITIONS", "1")
        );
        System.out.println(partitions);
        KafkaTopicManager topicManager =
            new KafkaTopicManager(
                bootstrapServers
        );

        topicManager.createTopicIfNotExists(
            topic,
            partitions,
            (short) 1
        );

        KafkaFlightProducer producer =
        new KafkaFlightProducer(bootstrapServers, topic);

        System.out.println(
            "Kafka topic: " + topic
        );
        System.out.println(
            "Kafka broker: " + bootstrapServers
        );

        HdfsFlightLoader loader =
                new HdfsFlightLoader();

        List<FlightEvent> events =
                loader.load(
                        hdfsUri,
                        hdfsFilePath
                );
        System.out.println(
                "Loaded events: "
                        + events.size()
        );
        ReplayEngine replayEngine =
                new ReplayEngine(20000, producer);

        replayEngine.replay(events);
        producer.close();

   
        
    }
}
