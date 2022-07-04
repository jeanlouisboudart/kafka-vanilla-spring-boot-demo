package com.example.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogAndFailExceptionHandler implements KafkaExceptionHandler {
    private final Logger logger = LoggerFactory.getLogger(LogAndFailExceptionHandler.class);

    private final OnFatalErrorListener defaultOnFatalErrorListener;

    public LogAndFailExceptionHandler() {
        this(new PropagateFatalErrorListener());
    }

    public LogAndFailExceptionHandler(OnFatalErrorListener onFatalErrorListener) {
        this.defaultOnFatalErrorListener = onFatalErrorListener;
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
        fireOnFatalErrorEvent(exception, onFatalErrorListener, ErrorType.PROCESSING_ERROR);
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
            fireOnFatalErrorEvent(record.key().getException(), onFatalErrorListener, ErrorType.DESERIALIZATION_ERROR);
            return;
        }

        if (record.key() != null && !record.value().valid()) {
            logger.warn("Exception caught during Deserialization of the value, topic: {}, partition: {}, offset: {}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value().getException());
            fireOnFatalErrorEvent(record.key().getException(), onFatalErrorListener, ErrorType.DESERIALIZATION_ERROR);
            return;
        }
        onValidRecordListener.onValidRecordEvent();
    }

    private void fireOnFatalErrorEvent(Exception exception, OnFatalErrorListener onFatalErrorListener, ErrorType errorType) {
        if (onFatalErrorListener != null) {
            onFatalErrorListener.onFatalErrorEvent(exception, errorType);
        } else {
            defaultOnFatalErrorListener.onFatalErrorEvent(exception, errorType);
        }
    }
}
