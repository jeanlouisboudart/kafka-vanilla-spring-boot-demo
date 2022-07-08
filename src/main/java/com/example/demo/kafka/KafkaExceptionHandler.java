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

    @FunctionalInterface
    interface OnValidRecordListener<K, V> {
        void onValidRecordEvent(ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record);
    }

    @FunctionalInterface
    interface OnSkippedRecordListener<K, V> {
        void onSkippedRecordEvent(ErrorType errorType, ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record, Exception exception);
    }

    @FunctionalInterface
    interface OnFatalErrorListener<K, V> {
        void onFatalErrorEvent(ErrorType errorType, ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record, Exception exception);
    }

    class NoOpOnSkippedRecordListener<K, V> implements OnSkippedRecordListener<K, V> {
        @Override
        public void onSkippedRecordEvent(ErrorType errorType, ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record, Exception exception) {

        }
    }

    class PropagateFatalErrorListener<K, V> implements OnFatalErrorListener<K, V> {

        @Override
        public void onFatalErrorEvent(ErrorType errorType, ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record, Exception exception) {
            throw new RuntimeException(exception);
        }
    }

    enum ErrorType {
        DESERIALIZATION_ERROR,
        PROCESSING_ERROR
    }
}
