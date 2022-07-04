package com.example.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogAndContinueExceptionHandler implements KafkaExceptionHandler {
    private final Logger logger = LoggerFactory.getLogger(LogAndContinueExceptionHandler.class);

    private final OnSkippedRecordListener defaultOnSkippedRecordListener;

    public LogAndContinueExceptionHandler() {
        this(new NoOpOnSkippedRecordListener());
    }

    public LogAndContinueExceptionHandler(OnSkippedRecordListener onSkippedRecordListener) {
        this.defaultOnSkippedRecordListener = onSkippedRecordListener;
    }

    @Override
    public <K, V> void handleProcessingError(
            ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record,
            Exception exception,
            OnSkippedRecordListener onSkippedRecordListener,
            OnFatalErrorListener onFatalErrorListener) {
        logger.warn("Exception caught during processing, topic: {}, partition: {}, offset: {}",
                record.topic(),
                record.partition(),
                record.offset(),
                exception);
        fireOnSkippedEvent(ErrorType.PROCESSING_ERROR, record, exception, onSkippedRecordListener);
    }

    @Override
    public <K, V> void handleDeserializationError(
            ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record,
            OnValidRecordListener onValidRecordListener,
            OnSkippedRecordListener onSkippedRecordListener,
            OnFatalErrorListener onFatalErrorListener) {
        if (record.key() != null && !record.key().valid()) {
            logger.warn("Exception caught during Deserialization of the key, topic: {}, partition: {}, offset: {}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key().getException());
            fireOnSkippedEvent(ErrorType.DESERIALIZATION_ERROR, record, record.key().getException(), onSkippedRecordListener);
            return;
        }

        if (record.value() != null && !record.value().valid()) {
            logger.warn("Exception caught during Deserialization of the value, topic: {}, partition: {}, offset: {}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value().getException());
            fireOnSkippedEvent(ErrorType.DESERIALIZATION_ERROR, record, record.value().getException(), onSkippedRecordListener);
            return;
        }
        onValidRecordListener.onValidRecordEvent();
    }

    private <K, V> void fireOnSkippedEvent(ErrorType errorType, ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record, Exception exception, OnSkippedRecordListener onSkippedRecordListener) {
        if (onSkippedRecordListener != null) {
            onSkippedRecordListener.onSkippedRecordEvent(errorType, record, exception);
        } else {
            defaultOnSkippedRecordListener.onSkippedRecordEvent(errorType, record, exception);
        }
    }
}
