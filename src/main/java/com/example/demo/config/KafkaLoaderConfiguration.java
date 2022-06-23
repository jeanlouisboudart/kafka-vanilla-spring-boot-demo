package com.example.demo.config;

import com.example.demo.kafka.DlqExceptionHandler;
import com.example.demo.kafka.KafkaConsumerWithErrorHandling;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@AllArgsConstructor
public class KafkaLoaderConfiguration {

    private final MeterRegistry meterRegistry;

    @Bean
    public KafkaProducer<?, ?> createKafkaProducer(KafkaConfig kafkaConfig) {
        KafkaProducer<?, ?> producer = new KafkaProducer<>(kafkaConfig.producerConfigs());
        new KafkaClientMetrics(producer).bindTo(meterRegistry);
        return producer;
    }

    @Bean
    public DlqExceptionHandler dlqExceptionHandler(KafkaConfig kafkaConfig, KafkaProducer<byte[], byte[]> dlqProducer) {
        return new DlqExceptionHandler(dlqProducer, kafkaConfig.getDlqName(), kafkaConfig.getAppName());
    }

    @Bean
    public KafkaProducer<byte[], byte[]> dlqProducer(KafkaConfig kafkaConfig) {
        return new KafkaProducer<>(kafkaConfig.dlqProducerConfigs());
    }

    @Bean
    public KafkaConsumerWithErrorHandling<?, ?> createKafkaConsumerWithDLQ(KafkaConfig kafkaConfig, DlqExceptionHandler dlqExceptionHandler) {
        KafkaConsumerWithErrorHandling<?, ?> consumer = new KafkaConsumerWithErrorHandling<>(kafkaConfig.consumerConfigs(), dlqExceptionHandler);
        new KafkaClientMetrics(consumer).bindTo(meterRegistry);
        return consumer;
    }


}
