package com.example.demo.config;

import com.example.demo.services.PaymentPublisher;
import com.google.common.collect.Lists;
import jakarta.annotation.PostConstruct;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@Configuration
@AllArgsConstructor
public class KafkaTopicInitializer {

    private final Logger logger = LoggerFactory.getLogger(KafkaTopicInitializer.class);

    private final KafkaConfig kafkaConfig;

    @PostConstruct
    public void initTopics() {
        List<String> listOfTopicToCreate = Lists.newArrayList(PaymentPublisher.TOPIC_NAME, kafkaConfig.getDlqName());

        try (AdminClient adminClient = KafkaAdminClient.create(kafkaConfig.kafkaConfigs())) {
            Set<String> existingTopics = adminClient.listTopics().names().get();
            List<NewTopic> newTopics = new ArrayList<>();
            for (String topic : listOfTopicToCreate) {
                if (!existingTopics.contains(topic)) {
                    logger.info("Creating topic {}", topic);
                    newTopics.add(new NewTopic(topic, 4, (short) 1));
                }
            }
            CreateTopicsResult topicsCreationResult = adminClient.createTopics(newTopics);
            topicsCreationResult.all().get();
        } catch (ExecutionException e) {
            //silent ignore if topic already exists
        } catch (InterruptedException e) {
            //noop
        }
    }
}
