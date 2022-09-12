package com.example.demo.config;

import com.example.demo.kafka.consumers.ErrorHandler;
import com.example.demo.kafka.consumers.ErrorWrapperDeserializer;
import com.example.demo.kafka.streams.KStreamDeserializationHandler;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
@ConfigurationProperties(prefix = "kafka")
public class KafkaConfig {

    public static final String DLQ_SUFFIX = "-dlq";
    @Setter
    private Map<String, String> properties;

    @Setter
    private Map<String, String> producer;

    @Setter
    private Map<String, String> consumer;

    @Setter
    private Map<String, String> streams;


    @Value("${spring.application.name}")
    @Getter
    @Setter
    private String appName;

    @Getter
    @Setter
    private ErrorHandler exceptionHandler = ErrorHandler.LogAndFail;

    @Setter
    private String dlqName;

    @Getter
    @Setter
    private int nbConsumerThreads = 1;

    private int nbConsumerCreated = 0;

    public String getDlqName() {
        return dlqName != null ? dlqName : appName + DLQ_SUFFIX;
    }

    public Map<String, Object> kafkaConfigs() {
        return new HashMap<>(properties);
    }

    public Map<String, Object> producerConfigs() {
        Map<String, Object> producerProps = new HashMap<>(properties);
        producerProps.putIfAbsent(ProducerConfig.CLIENT_ID_CONFIG, appName);
        producerProps.putAll(producer);
        return producerProps;
    }

    public Map<String, Object> consumerConfigs() {
        Map<String, Object> consumerProps = new HashMap<>(properties);
        consumerProps.putAll(consumer);
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, buildConsumerClientId((String) consumerProps.getOrDefault(ConsumerConfig.CLIENT_ID_CONFIG, appName + "-consumer")));
        consumerProps.putIfAbsent(ErrorWrapperDeserializer.KEY_WRAPPER_DESERIALIZER_CLASS, consumerProps.getOrDefault(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName()));
        consumerProps.putIfAbsent(ErrorWrapperDeserializer.VALUE_WRAPPER_DESERIALIZER_CLASS, consumerProps.getOrDefault(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName()));

        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorWrapperDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorWrapperDeserializer.class);

        return consumerProps;
    }

    public Map<String, Object> streamsConfig() {
        Map<String, Object> streamsProps = new HashMap<>(properties);
        streamsProps.putIfAbsent(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, KStreamDeserializationHandler.class);
        streamsProps.putAll(streams);
        return streamsProps;
    }

    public String buildConsumerClientId(String clientId) {
        return nbConsumerThreads == 1 ? clientId : clientId + "-" + nbConsumerCreated++;
    }


    public Map<String, Object> dlqProducerConfigs() {
        Map<String, Object> dlqConfig = new HashMap<>(properties);
        dlqConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        dlqConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        dlqConfig.put(ProducerConfig.CLIENT_ID_CONFIG, appName + DLQ_SUFFIX);
        return dlqConfig;
    }

}
