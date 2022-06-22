package com.example.demo.config;

import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
@ConfigurationProperties(prefix = "kafka")
public class KafkaConfig {
    @Setter
    private Map<String, Object> properties;

    @Setter
    private Map<String, Object> producer;

    @Setter
    private Map<String, Object> consumer;

    @Value("${spring.application.name}")
    @Getter
    @Setter
    private String appName;

    public Map<String, Object> producerConfigs() {
        Map<String, Object> producerProps = new HashMap<>(properties);
        producerProps.putIfAbsent(ProducerConfig.CLIENT_ID_CONFIG, appName);
        producerProps.putAll(producer);
        return producerProps;
    }

    public Map<String, Object> consumerConfigs() {
        Map<String, Object> consumerProps = new HashMap<>(properties);
        consumerProps.putIfAbsent(ConsumerConfig.CLIENT_ID_CONFIG, appName);
        consumerProps.putAll(consumer);
        return consumerProps;
    }

    public Map<String, Object> dlqProducerConfigs() {
        Map<String, Object> dlqConfig = new HashMap<>(properties);
        dlqConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        dlqConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        dlqConfig.put(ProducerConfig.CLIENT_ID_CONFIG, appName + dlqConfig);
        return dlqConfig;
    }

}
