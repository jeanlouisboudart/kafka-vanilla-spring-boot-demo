package com.example.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface KafkaExceptionHandler<K, V> {

    void handleProcessingError(
            final ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record,
            Exception exception,
            OnSkippedRecordListener<K, V> onSkippedRecordListener,
            OnFatalErrorListener<K, V> onFatalErrorListener);

    default void handleProcessingError(
            final ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record,
            Exception exception) {
        handleProcessingError(record, exception, null, null);
    }

    void handleDeserializationError(
            final ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record,
            OnValidRecordListener<K, V> onValidRecordListener,
            OnSkippedRecordListener<K, V> onSkippedRecordListener,
            OnFatalErrorListener<K, V> onFatalErrorListener);

    default void handleDeserializationError(
            final ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record,
            OnValidRecordListener<K, V> onValidRecordListener) {
        handleDeserializationError(record, onValidRecordListener, null, null);
    }

}
