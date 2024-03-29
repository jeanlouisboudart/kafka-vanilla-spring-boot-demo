package com.example.demo.kafka.consumers;

import com.example.demo.kafka.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Instant;

import static com.example.demo.kafka.DlqUtils.*;

public class DlqExceptionHandler<K, V> implements KafkaExceptionHandler<K, V>, Closeable {
    private final Logger logger = LoggerFactory.getLogger(DlqExceptionHandler.class);
    private final Producer<byte[], byte[]> producer;
    private final String dlqTopicName;
    private final String appName;
    private final boolean failWhenErrorWritingToDlq;
    private final OnSkippedRecordListener<K, V> defaultOnSkippedRecordListener;
    private final OnFatalErrorListener<K, V> defaultOnFatalErrorListener;


    public DlqExceptionHandler(Producer<byte[], byte[]> producer, String dlqTopicName, String appName) {
        this(producer, dlqTopicName, appName, true);
    }

    public DlqExceptionHandler(Producer<byte[], byte[]> producer,
                               String dlqTopicName,
                               String appName,
                               boolean failWhenErrorWritingToDlq) {
        this(producer, dlqTopicName, appName, failWhenErrorWritingToDlq, new NoOpOnSkippedRecordListener<>(), new PropagateFatalErrorListener<>());
    }

    public DlqExceptionHandler(Producer<byte[], byte[]> producer,
                               String dlqTopicName,
                               String appName,
                               boolean failWhenErrorWritingToDlq,
                               OnSkippedRecordListener<K, V> onSkippedRecordListener,
                               OnFatalErrorListener<K, V> onFatalErrorListener) {
        this.producer = producer;
        this.dlqTopicName = dlqTopicName;
        this.appName = appName;
        this.failWhenErrorWritingToDlq = failWhenErrorWritingToDlq;
        this.defaultOnSkippedRecordListener = onSkippedRecordListener;
        this.defaultOnFatalErrorListener = onFatalErrorListener;
    }


    @Override
    public void handleProcessingError(
            ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record,
            Exception exception,
            OnSkippedRecordListener<K, V> onSkippedRecordListener,
            OnFatalErrorListener<K, V> onFatalErrorListener) {
        logger.warn("Exception caught during processing, topic: {}, partition: {}, offset: {}",
                record.topic(),
                record.partition(),
                record.offset(),
                exception);
        sendToDlq(ErrorType.PROCESSING_ERROR, record, exception, onSkippedRecordListener, onFatalErrorListener);

    }

    private void errorWhileWritingToDLQ(ErrorType errorType, ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record, Exception e, OnSkippedRecordListener<K, V> onSkippedRecordListener, OnFatalErrorListener<K, V> onFatalErrorListener) {
        if (failWhenErrorWritingToDlq) {
            logger.error("Could not send to dlq, topic: {}, partition: {}, offset: {}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    e);
            fireOnFatalErrorEvent(errorType, record, e, onFatalErrorListener);
        } else {
            logger.warn("Could not send to dlq, topic: {}, partition: {}, offset: {}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    e);
            fireOnSkippedEvent(errorType, record, e, onSkippedRecordListener);
        }
    }

    @Override
    public void handleDeserializationError(
            ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record,
            OnValidRecordListener<K, V> onValidRecordListener,
            OnSkippedRecordListener<K, V> onSkippedRecordListener,
            OnFatalErrorListener<K, V> onFatalErrorListener) {
        if (record.key() != null && !record.key().valid()) {
            logger.warn("Exception caught during Deserialization of the key, topic: {}, partition: {}, offset: {}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key().getException());

            sendToDlq(ErrorType.DESERIALIZATION_ERROR, record, record.key().getException(), onSkippedRecordListener, onFatalErrorListener);
            return;
        }

        if (record.value() != null && !record.value().valid()) {
            logger.warn("Exception caught during Deserialization of the value, topic: {}, partition: {}, offset: {}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value().getException());
            sendToDlq(ErrorType.DESERIALIZATION_ERROR, record, record.value().getException(), onSkippedRecordListener, onFatalErrorListener);
            return;

        }

        onValidRecordListener.onValidRecordEvent(record);
    }

    private void sendToDlq(ErrorType errorType,
                           ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record,
                           Exception exception,
                           OnSkippedRecordListener<K, V> onSkippedRecordListener,
                           OnFatalErrorListener<K, V> onFatalErrorListener) {
        Headers headers = record.headers();
        addHeader(headers, DLQ_HEADER_APP_NAME, appName);
        addHeader(headers, DLQ_HEADER_TOPIC, record.topic());
        addHeader(headers, DLQ_HEADER_PARTITION, record.partition());
        addHeader(headers, DLQ_HEADER_OFFSET, record.offset());
        addHeader(headers, DLQ_HEADER_TIMESTAMP, Instant.now().toString());
        addHeader(headers, DLQ_HEADER_EXCEPTION_CLASS, exception.getClass().getCanonicalName());
        addHeader(headers, DLQ_HEADER_EXCEPTION_MESSAGE, exception.getMessage());
        addHeader(headers, DLQ_HEADER_ERROR_TYPE, errorType.name());


        byte[] key = record.key() != null ? record.key().getValueAsBytes() : null;
        byte[] value = record.value() != null ? record.value().getValueAsBytes() : null;
        //Here we use synchronous send because we want to make sure we wrote to DLQ before moving forward.
        try {
            producer.send(new ProducerRecord<>(dlqTopicName, null, record.timestamp(), key, value, headers)).get();
            fireOnSkippedEvent(errorType, record, exception, onSkippedRecordListener);
        } catch (Exception e) {
            errorWhileWritingToDLQ(errorType, record, e, onSkippedRecordListener, onFatalErrorListener);
        }
    }

    private void fireOnSkippedEvent(ErrorType errorType,
                                    ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record,
                                    Exception exception,
                                    OnSkippedRecordListener<K, V> onSkippedRecordListener) {
        if (onSkippedRecordListener != null) {
            onSkippedRecordListener.onSkippedRecordEvent(errorType, record, exception);
        } else {
            defaultOnSkippedRecordListener.onSkippedRecordEvent(errorType, record, exception);
        }
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

    @Override
    public void close() {
        logger.info("Closing dlq producer");
        producer.close();
    }
}
