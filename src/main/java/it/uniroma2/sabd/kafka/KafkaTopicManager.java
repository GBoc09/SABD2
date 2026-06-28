package it.uniroma2.sabd.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;

public class KafkaTopicManager {

    private final String bootstrapServers;

    public KafkaTopicManager(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public void createTopicIfNotExists(
            String topicName,
            int partitions,
            short replicationFactor)
            throws Exception {

        Properties properties =
                new Properties();

        properties.put(
                "bootstrap.servers",
                bootstrapServers
        );

        try (AdminClient adminClient =
                     AdminClient.create(properties)) {

            ListTopicsResult topics =
                    adminClient.listTopics();

            Set<String> existingTopics =
                    topics.names().get();

            if (existingTopics.contains(topicName)) {
                TopicDescription topicDescription =
                        adminClient.describeTopics(
                                Collections.singletonList(topicName)
                        ).topicNameValues().get(topicName).get();

                int existingPartitions =
                        topicDescription.partitions().size();

                if (existingPartitions < partitions) {
                    adminClient.createPartitions(
                            Collections.singletonMap(
                                    topicName,
                                    NewPartitions.increaseTo(partitions)
                            )
                    ).all().get();

                    System.out.println(
                            "Topic partitions increased: "
                                    + topicName
                                    + " from "
                                    + existingPartitions
                                    + " to "
                                    + partitions
                    );
                } else {
                    System.out.println(
                            "Topic already exists: "
                                    + topicName
                                    + " (Partitions: "
                                    + existingPartitions
                                    + ")"
                    );
                }

                return;
            }

            NewTopic topic =
                    new NewTopic(
                            topicName,
                            partitions,
                            replicationFactor
                    );

            adminClient.createTopics(
                    Collections.singletonList(topic)
            ).all().get();

            System.out.println(
                    "Topic created successfully: "
                            + topicName
            );
        }
    }
}
