package com.example.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface KafkaExceptionHandler {

    <K, V> void handleProcessingError(
            final ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record,
            Exception exception,
            OnSkippedRecord onSkippedRecord,
            OnFatalError onFatalError);

    default <K, V> void handleProcessingError(
            final ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record,
            Exception exception,
            OnFatalError onFatalError) {
        handleProcessingError(record, exception, (e) -> {
        }, onFatalError);
    }

    <K, V> void handleDeserializationError(
            final ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record,
            OnValidRecord onValidRecord,
            OnSkippedRecord onSkippedRecord,
            OnFatalError onFatalError);

    default <K, V> void handleDeserializationError(
            final ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record,
            OnValidRecord onValidRecord,
            OnFatalError onFatalError) {
        handleDeserializationError(record, onValidRecord, (e) -> {
        }, onFatalError);
    }

    @FunctionalInterface
    interface OnValidRecord {
        void handle();
    }

    @FunctionalInterface
    interface OnSkippedRecord {
        void handle(Exception exception);
    }

    @FunctionalInterface
    interface OnFatalError {
        void handle(Exception exception);
    }
}
