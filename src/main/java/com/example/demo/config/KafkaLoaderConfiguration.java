package com.example.demo.config;

import com.example.demo.kafka.*;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Optional;

import static com.example.demo.config.KafkaConfig.*;

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
    public KafkaExceptionHandler kafkaExceptionHandler(KafkaConfig kafkaConfig, KafkaProducer<byte[], byte[]> dlqProducer) {
        String handlerType = Optional.ofNullable(kafkaConfig.getExceptionHandler()).orElseThrow(() -> new IllegalStateException("exception handler not configured"));
        switch (handlerType) {
            case LOG_AND_CONTINUE:
                return new LogAndContinueExceptionHandler();
            case LOG_AND_FAIL:
                return new LogAndFailExceptionHandler();
            case DEAD_LETTER_QUEUE:
                return new DlqExceptionHandler(dlqProducer, kafkaConfig.getDlqName(), kafkaConfig.getAppName());
            default:
                throw new IllegalStateException("unknown exception handler: " + handlerType);
        }
    }

    @Bean
    public KafkaProducer<byte[], byte[]> dlqProducer(KafkaConfig kafkaConfig) {
        return new KafkaProducer<>(kafkaConfig.dlqProducerConfigs());
    }

    @Bean
    public KafkaConsumer<?, ?> createKafkaConsumerWithDLQ(KafkaConfig kafkaConfig) {
        KafkaConsumer<DeserializerResult<?>, DeserializerResult<?>> consumer = new KafkaConsumer<>(kafkaConfig.consumerConfigs());
        new KafkaClientMetrics(consumer).bindTo(meterRegistry);
        return consumer;
    }


}
