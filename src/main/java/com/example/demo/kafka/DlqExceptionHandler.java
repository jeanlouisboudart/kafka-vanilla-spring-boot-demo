package com.example.demo.kafka;

import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Instant;

import static com.example.demo.kafka.DlqUtils.*;

@AllArgsConstructor
public class DlqExceptionHandler implements KafkaExceptionHandler, Closeable {
    private final Logger logger = LoggerFactory.getLogger(DlqExceptionHandler.class);
    private final Producer<byte[], byte[]> producer;
    private final String dlqTopicName;
    private final String appName;

    @Override
    public <K, V> DeserializationHandlerResponse handleProcessingError(ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record, Exception exception) {
        logger.warn("Exception caught during processing, topic: {}, partition: {}, offset: {}",
                record.topic(),
                record.partition(),
                record.offset(),
                exception);
        sendToDlq(record, exception);
        return DeserializationHandlerResponse.IGNORE;
    }

    @Override
    public <K, V> DeserializationHandlerResponse handleDeserializationError(ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record) {
        if (record.key() != null && !record.key().valid()) {
            logger.warn("Exception caught during Deserialization of the key, topic: {}, partition: {}, offset: {}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key().getException());

            sendToDlq(record, record.key().getException());
            return DeserializationHandlerResponse.IGNORE;
        }

        if (record.value() != null && !record.value().valid()) {
            logger.warn("Exception caught during Deserialization of the value, topic: {}, partition: {}, offset: {}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value().getException());

            sendToDlq(record, record.value().getException());
            return DeserializationHandlerResponse.IGNORE;
        }


        return DeserializationHandlerResponse.VALID;
    }

    private <K, V> void sendToDlq(ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record, Exception exception) {
        Headers headers = record.headers();
        addHeader(headers, DLQ_HEADER_APP_NAME, appName);
        addHeader(headers, DLQ_HEADER_TOPIC, record.topic());
        addHeader(headers, DLQ_HEADER_PARTITION, record.partition());
        addHeader(headers, DLQ_HEADER_OFFSET, record.offset());
        addHeader(headers, DLQ_HEADER_TIMESTAMP, Instant.now().toString());
        addHeader(headers, DLQ_HEADER_EXCEPTION_CLASS, exception.getClass().getCanonicalName());
        addHeader(headers, DLQ_HEADER_EXCEPTION_MESSAGE, exception.getMessage());

        try {

            byte[] key = record.key() != null ? record.key().getValueAsBytes() : null;
            byte[] value = record.value() != null ? record.value().getValueAsBytes() : null;
            //Here we use synchronous send because we want to make sure we wrote to DLQ before moving forward.
            producer.send(new ProducerRecord<>(dlqTopicName, null, record.timestamp(), key, value, headers)).get();
        } catch (Exception e) {
            logger.error("Could not send to dlq, topic: {}, partition: {}, offset: {}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    e);
        }
    }

    @Override
    public void close() {
        logger.info("Closing dlq producer");
        producer.close();
    }
}
