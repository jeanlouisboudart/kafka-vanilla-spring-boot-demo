package com.example.demo.kafka.consumers;

import com.example.demo.kafka.ErrorType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogAndFailExceptionHandler<K, V> implements KafkaExceptionHandler<K, V> {
    private final Logger logger = LoggerFactory.getLogger(LogAndFailExceptionHandler.class);

    private final OnFatalErrorListener<K, V> defaultOnFatalErrorListener;

    public LogAndFailExceptionHandler() {
        this(new PropagateFatalErrorListener<>());
    }

    public LogAndFailExceptionHandler(OnFatalErrorListener<K, V> onFatalErrorListener) {
        this.defaultOnFatalErrorListener = onFatalErrorListener;
    }

    @Override
    public void handleProcessingError(
            ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record,
            Exception exception,
            OnSkippedRecordListener<K, V> onSkippedRecordListener,
            OnFatalErrorListener<K, V> onFatalErrorListener) {
        logger.error("Exception caught during processing, topic: " + record.topic() + ", partition: " + record.partition() + ", offset: " + record.offset(), exception);
        fireOnFatalErrorEvent(ErrorType.PROCESSING_ERROR, record, exception, onFatalErrorListener);
    }

    @Override
    public void handleDeserializationError(
            ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record,
            OnValidRecordListener<K, V> onValidRecordListener,
            OnSkippedRecordListener<K, V> onSkippedRecordListener,
            OnFatalErrorListener<K, V> onFatalErrorListener) {
        if (record.key() != null && !record.key().valid()) {
            logger.error("Exception caught during Deserialization of the key, topic: " + record.topic() + ", partition: " + record.partition() + ", offset: " + record.offset(), record.key().getException());
            fireOnFatalErrorEvent(ErrorType.DESERIALIZATION_ERROR, record, record.key().getException(), onFatalErrorListener);
            return;
        }

        if (record.value() != null && !record.value().valid()) {
            logger.error("Exception caught during Deserialization of the value, topic: " + record.topic() + ", partition: " + record.partition() + ", offset: " + record.offset(), record.value().getException());
            fireOnFatalErrorEvent(ErrorType.DESERIALIZATION_ERROR, record, record.value().getException(), onFatalErrorListener);
            return;
        }
        onValidRecordListener.onValidRecordEvent(record);
    }

    private void fireOnFatalErrorEvent(ErrorType errorType,
                                       ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record,
                                       Exception exception,
                                       OnFatalErrorListener<K, V> onFatalErrorListener) {
        if (onFatalErrorListener != null) {
            onFatalErrorListener.onFatalErrorEvent(errorType, record, exception);
        } else {
            defaultOnFatalErrorListener.onFatalErrorEvent(errorType, record, exception);
        }
    }
}
