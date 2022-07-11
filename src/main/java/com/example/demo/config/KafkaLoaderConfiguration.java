package com.example.demo.config;

import com.example.demo.kafka.*;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Optional;

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
    public <K, V> OnSkippedRecordListener<K, V> defaultOnSkippedListener(KafkaErrorHandlerMetrics kafkaErrorHandlerMetrics) {
        return new OnSkippedRecordListener<>() {

            @Override
            public void onSkippedRecordEvent(ErrorType errorType, ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record, Exception exception) {
                kafkaErrorHandlerMetrics.totalSkippedRecords().increment();
                kafkaErrorHandlerMetrics.totalSkippedRecords(errorType, exception).increment();
            }
        };
    }


    @Bean
    public <K, V> OnFatalErrorListener<K, V> defaultOnFatalErrorListener(KafkaErrorHandlerMetrics kafkaErrorHandlerMetrics) {

        return new OnFatalErrorListener<>() {
            @Override
            public void onFatalErrorEvent(ErrorType errorType, ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record, Exception exception) {
                kafkaErrorHandlerMetrics.totalFatalError().increment();
                kafkaErrorHandlerMetrics.totalFatalError(errorType, exception).increment();
                //By default, we propagate the exception but here you can customize the global behavior like shutting down the application if you want to have fail-fast approach.
                throw new RuntimeException(exception);
            }
        };
    }

    @Bean
    public <K, V> KafkaExceptionHandler<K, V> kafkaExceptionHandler(KafkaConfig kafkaConfig, KafkaProducer<byte[], byte[]> dlqProducer, OnSkippedRecordListener<K, V> defaultOnSkippedListener, OnFatalErrorListener<K, V> defaultOnFatalErrorListener) {
        ErrorHandler handlerType = Optional.ofNullable(kafkaConfig.getExceptionHandler()).orElseThrow(() -> new IllegalStateException("exception handler not configured"));
        switch (handlerType) {
            case LogAndContinue:
                return new LogAndContinueExceptionHandler<>(defaultOnSkippedListener);
            case LogAndFail:
                return new LogAndFailExceptionHandler<>(defaultOnFatalErrorListener);
            case DeadLetterQueue:
                return new DlqExceptionHandler<>(dlqProducer, kafkaConfig.getDlqName(), kafkaConfig.getAppName(), true, defaultOnSkippedListener, defaultOnFatalErrorListener);
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
