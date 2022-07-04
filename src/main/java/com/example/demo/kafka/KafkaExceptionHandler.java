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
            Exception exception) {
        handleProcessingError(record, exception, null, null);
    }

    <K, V> void handleDeserializationError(
            final ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record,
            OnValidRecordListener onValidRecordListener,
            OnSkippedRecordListener onSkippedRecordListener,
            OnFatalErrorListener onFatalErrorListener);

    default <K, V> void handleDeserializationError(
            final ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record,
            OnValidRecordListener onValidRecordListener) {
        handleDeserializationError(record, onValidRecordListener, null, null);
    }

    @FunctionalInterface
    interface OnValidRecordListener {
        void onValidRecordEvent();
    }

    @FunctionalInterface
    interface OnSkippedRecordListener {
        <K, V> void onSkippedRecordEvent(ErrorType errorType, ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record, Exception exception);
    }

    @FunctionalInterface
    interface OnFatalErrorListener {
        <K, V> void onFatalErrorEvent(ErrorType errorType, ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record, Exception exception);
    }

    class NoOpOnSkippedRecordListener implements OnSkippedRecordListener {
        @Override
        public <K, V> void onSkippedRecordEvent(ErrorType errorType, ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record, Exception exception) {

        }
    }

    class PropagateFatalErrorListener implements OnFatalErrorListener {

        @Override
        public <K, V> void onFatalErrorEvent(ErrorType errorType, ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record, Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    enum ErrorType {
        DESERIALIZATION_ERROR,
        PROCESSING_ERROR
    }
}
