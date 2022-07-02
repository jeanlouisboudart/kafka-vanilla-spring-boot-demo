package com.example.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface KafkaExceptionHandler {

    <K, V> void handleProcessingError(
            final ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record,
            Exception exception,
            OnSkippedRecordListener onSkippedRecordListener,
            OnFatalErrorListener onFatalErrorListener);

    default <K, V> void handleProcessingError(
            final ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record,
            Exception exception,
            OnFatalErrorListener onFatalErrorListener) {
        handleProcessingError(record, exception, (e) -> {
        }, onFatalErrorListener);
    }

    <K, V> void handleDeserializationError(
            final ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record,
            OnValidRecordListener onValidRecordListener,
            OnSkippedRecordListener onSkippedRecordListener,
            OnFatalErrorListener onFatalErrorListener);

    default <K, V> void handleDeserializationError(
            final ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record,
            OnValidRecordListener onValidRecordListener,
            OnFatalErrorListener onFatalErrorListener) {
        handleDeserializationError(record, onValidRecordListener, (e) -> {
        }, onFatalErrorListener);
    }

    @FunctionalInterface
    interface OnValidRecordListener {
        void onValidRecordEvent();
    }

    @FunctionalInterface
    interface OnSkippedRecordListener {
        void onSkippedRecordEvent(Exception exception);
    }

    @FunctionalInterface
    interface OnFatalErrorListener {
        void onFatalErrorEvent(Exception exception);
    }

}
