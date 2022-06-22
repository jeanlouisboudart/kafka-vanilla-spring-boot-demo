package com.example.demo.config;

import com.example.demo.kafka.DlqExceptionHandler;
import com.example.demo.kafka.KafkaConsumerWithErrorHandling;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaLoaderConfiguration {

    @Bean
    public KafkaProducer<?, ?> createKafkaProducer(KafkaConfig kafkaConfig) {
        return new KafkaProducer<>(kafkaConfig.producerConfigs());
    }

    @Bean
    public DlqExceptionHandler dlqExceptionHandler(KafkaConfig kafkaConfig) {
        return new DlqExceptionHandler(new KafkaProducer<>(kafkaConfig.dlqProducerConfigs()), "demo-app-dlq");
    }

    @Bean
    public KafkaConsumerWithErrorHandling<?, ?> createKafkaConsumerWithDLQ(KafkaConfig kafkaConfig, DlqExceptionHandler dlqExceptionHandler) {
        return new KafkaConsumerWithErrorHandling<>(kafkaConfig.consumerConfigs(), dlqExceptionHandler);
    }


}
