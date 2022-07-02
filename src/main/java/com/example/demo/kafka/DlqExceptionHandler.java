package com.example.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Instant;
import java.util.concurrent.ExecutionException;

import static com.example.demo.kafka.DlqUtils.*;

public class DlqExceptionHandler implements KafkaExceptionHandler, Closeable {
    private final Logger logger = LoggerFactory.getLogger(DlqExceptionHandler.class);
    private final Producer<byte[], byte[]> producer;
    private final String dlqTopicName;
    private final String appName;
    private final boolean failWhenErrorWritingToDlq;
    private final OnSkippedRecordListener defaultOnSkippedRecordListener;
    private final OnFatalErrorListener defaultOnFatalErrorListener;


    public DlqExceptionHandler(Producer<byte[], byte[]> producer, String dlqTopicName, String appName) {
        this(producer, dlqTopicName, appName, true);
    }

    public DlqExceptionHandler(Producer<byte[], byte[]> producer,
                               String dlqTopicName,
                               String appName,
                               boolean failWhenErrorWritingToDlq) {
        this(producer, dlqTopicName, appName, failWhenErrorWritingToDlq, new NoOpOnSkippedRecordListener(), new PropagateFatalErrorListener());
    }

    public DlqExceptionHandler(Producer<byte[], byte[]> producer,
                               String dlqTopicName,
                               String appName,
                               boolean failWhenErrorWritingToDlq,
                               OnSkippedRecordListener onSkippedRecordListener,
                               OnFatalErrorListener onFatalErrorListener) {
        this.producer = producer;
        this.dlqTopicName = dlqTopicName;
        this.appName = appName;
        this.failWhenErrorWritingToDlq = failWhenErrorWritingToDlq;
        this.defaultOnSkippedRecordListener = onSkippedRecordListener;
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
        try {
            sendToDlq(record, exception);
            fireOnSkippedEvent(exception, onSkippedRecordListener);
        } catch (Exception e) {
            errorWhileWritingToDLQ(record, e, onSkippedRecordListener, onFatalErrorListener);
        }
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

            try {
                sendToDlq(record, record.key().getException());
                fireOnSkippedEvent(record.key().getException(), onSkippedRecordListener);
            } catch (Exception e) {
                errorWhileWritingToDLQ(record, e, onSkippedRecordListener, onFatalErrorListener);
            }
            return;
        }

        if (record.value() != null && !record.value().valid()) {
            logger.warn("Exception caught during Deserialization of the value, topic: {}, partition: {}, offset: {}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value().getException());
            try {
                sendToDlq(record, record.value().getException());
                fireOnSkippedEvent(record.value().getException(), onSkippedRecordListener);
            } catch (Exception e) {
                errorWhileWritingToDLQ(record, e, onSkippedRecordListener, onFatalErrorListener);
            }
            return;

        }

        onValidRecordListener.onValidRecordEvent();
    }

    private <K, V> void errorWhileWritingToDLQ(ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record, Exception e, OnSkippedRecordListener onSkippedRecordListener, OnFatalErrorListener onFatalErrorListener) {
        logger.error("Could not send to dlq, topic: {}, partition: {}, offset: {}",
                record.topic(),
                record.partition(),
                record.offset(),
                e);
        if (failWhenErrorWritingToDlq) {
            fireOnFatalErrorEvent(e, onFatalErrorListener);
        } else {
            fireOnSkippedEvent(e, onSkippedRecordListener);
        }
    }

    private <K, V> void sendToDlq(ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record, Exception exception) throws ExecutionException, InterruptedException {
        Headers headers = record.headers();
        addHeader(headers, DLQ_HEADER_APP_NAME, appName);
        addHeader(headers, DLQ_HEADER_TOPIC, record.topic());
        addHeader(headers, DLQ_HEADER_PARTITION, record.partition());
        addHeader(headers, DLQ_HEADER_OFFSET, record.offset());
        addHeader(headers, DLQ_HEADER_TIMESTAMP, Instant.now().toString());
        addHeader(headers, DLQ_HEADER_EXCEPTION_CLASS, exception.getClass().getCanonicalName());
        addHeader(headers, DLQ_HEADER_EXCEPTION_MESSAGE, exception.getMessage());


        byte[] key = record.key() != null ? record.key().getValueAsBytes() : null;
        byte[] value = record.value() != null ? record.value().getValueAsBytes() : null;
        //Here we use synchronous send because we want to make sure we wrote to DLQ before moving forward.
        producer.send(new ProducerRecord<>(dlqTopicName, null, record.timestamp(), key, value, headers)).get();
    }

    private void fireOnSkippedEvent(Exception exception, OnSkippedRecordListener onSkippedRecordListener) {
        if (onSkippedRecordListener != null) {
            onSkippedRecordListener.onSkippedRecordEvent(exception);
        } else {
            defaultOnSkippedRecordListener.onSkippedRecordEvent(exception);
        }
    }

    private void fireOnFatalErrorEvent(Exception exception, OnFatalErrorListener onFatalErrorListener) {
        if (onFatalErrorListener != null) {
            onFatalErrorListener.onFatalErrorEvent(exception);
        } else {
            defaultOnFatalErrorListener.onFatalErrorEvent(exception);
        }
    }

    @Override
    public void close() {
        logger.info("Closing dlq producer");
        producer.close();
    }
}
