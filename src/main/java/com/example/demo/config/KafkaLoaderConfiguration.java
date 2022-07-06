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
    public KafkaExceptionHandler.OnSkippedRecordListener defaultOnSkippedListener(KafkaErrorHandlerMetrics kafkaErrorHandlerMetrics) {
        return new KafkaExceptionHandler.OnSkippedRecordListener() {

            @Override
            public <K, V> void onSkippedRecordEvent(KafkaExceptionHandler.ErrorType errorType, ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record, Exception exception) {
                kafkaErrorHandlerMetrics.totalSkippedRecords().increment();
                kafkaErrorHandlerMetrics.totalSkippedRecords(errorType, exception).increment();
            }
        };
    }


    @Bean
    public KafkaExceptionHandler.OnFatalErrorListener defaultOnFatalErrorListener(KafkaErrorHandlerMetrics kafkaErrorHandlerMetrics) {

        return new KafkaExceptionHandler.OnFatalErrorListener() {
            @Override
            public <K, V> void onFatalErrorEvent(KafkaExceptionHandler.ErrorType errorType, ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record, Exception exception) {
                kafkaErrorHandlerMetrics.totalFatalError().increment();
                kafkaErrorHandlerMetrics.totalFatalError(errorType, exception).increment();
                //By default, we propagate the exception but here you can customize the global behavior like shutting down the application if you want to have fail-fast approach.
                throw new RuntimeException(exception);
            }
        };
    }

    @Bean
    public KafkaExceptionHandler kafkaExceptionHandler(KafkaConfig kafkaConfig, KafkaProducer<byte[], byte[]> dlqProducer, KafkaExceptionHandler.OnSkippedRecordListener defaultOnSkippedListener, KafkaExceptionHandler.OnFatalErrorListener defaultOnFatalErrorListener) {
        String handlerType = Optional.ofNullable(kafkaConfig.getExceptionHandler()).orElseThrow(() -> new IllegalStateException("exception handler not configured"));
        switch (handlerType) {
            case LOG_AND_CONTINUE:
                return new LogAndContinueExceptionHandler(defaultOnSkippedListener);
            case LOG_AND_FAIL:
                return new LogAndFailExceptionHandler(defaultOnFatalErrorListener);
            case DEAD_LETTER_QUEUE:
                return new DlqExceptionHandler(dlqProducer, kafkaConfig.getDlqName(), kafkaConfig.getAppName(), true, defaultOnSkippedListener, defaultOnFatalErrorListener);
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
