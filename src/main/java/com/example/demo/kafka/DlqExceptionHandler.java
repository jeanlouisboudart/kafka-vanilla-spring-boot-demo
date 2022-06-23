package com.example.demo.kafka;

import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
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
    private final KafkaProducer<byte[], byte[]> producer;
    private final String dlqTopicName;
    private final String appName;

    @Override
    public <K, V> DeserializationHandlerResponse handleProcessingError(ConsumerRecord<K, V> record, Exception exception) {
        logger.warn("Exception caught during Processing, topic: {}, partition: {}, offset: {}",
                record.topic(),
                record.partition(),
                record.offset(),
                exception);

        //KafkaConsumerWithErrorHandling add the key & value in headers to preserve the original byte[]
        Headers headers = record.headers();
        byte[] key = headers.lastHeader(DLQ_HEADER_MESSAGE_KEY).value();
        byte[] value = headers.lastHeader(DLQ_HEADER_MESSAGE_VALUE).value();
        removeHeader(headers, DLQ_HEADER_MESSAGE_KEY);
        removeHeader(headers, DLQ_HEADER_MESSAGE_VALUE);

        sendToDlq(record, key, value, headers, exception);

        return DeserializationHandlerResponse.CONTINUE;
    }

    @Override
    public DeserializationHandlerResponse handleDeserializationError(ConsumerRecord<byte[], byte[]> record, Exception exception) {
        logger.warn("Exception caught during Deserialization, topic: {}, partition: {}, offset: {}",
                record.topic(),
                record.partition(),
                record.offset(),
                exception);

        sendToDlq(record, record.key(), record.value(), record.headers(), exception);

        return DeserializationHandlerResponse.CONTINUE;
    }

    private void sendToDlq(ConsumerRecord<?, ?> record, byte[] key, byte[] value, Headers headers, Exception exception) {
        addHeader(headers, DLQ_HEADER_APP_NAME, appName);
        addHeader(headers, DLQ_HEADER_TOPIC, record.topic());
        addHeader(headers, DLQ_HEADER_PARTITION, record.partition());
        addHeader(headers, DLQ_HEADER_OFFSET, record.offset());
        addHeader(headers, DLQ_HEADER_TIMESTAMP, Instant.now().toString());
        addHeader(headers, DLQ_HEADER_EXCEPTION_CLASS, exception.getClass().getCanonicalName());
        addHeader(headers, DLQ_HEADER_EXCEPTION_MESSAGE, exception.getMessage());

        removeHeader(headers, DLQ_HEADER_MESSAGE_KEY);
        removeHeader(headers, DLQ_HEADER_MESSAGE_VALUE);

        try {

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
