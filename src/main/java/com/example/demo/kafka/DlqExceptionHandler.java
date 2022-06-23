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

@AllArgsConstructor
public class DlqExceptionHandler implements DeserializationExceptionHandler, Closeable {
    private final Logger logger = LoggerFactory.getLogger(DlqExceptionHandler.class);
    private static  final String DLQ_HEADER_PREFIX = "dlq.error.";
    private static  final String DLQ_HEADER_APP_NAME = DLQ_HEADER_PREFIX + "app.name";
    private static  final String DLQ_HEADER_TIMESTAMP = DLQ_HEADER_PREFIX + "timestamp";
    private static  final String DLQ_HEADER_TOPIC = DLQ_HEADER_PREFIX + "topic";
    private static  final String DLQ_HEADER_PARTITION = DLQ_HEADER_PREFIX + "partition";
    private static  final String DLQ_HEADER_OFFSET = DLQ_HEADER_PREFIX + "offset";
    private static  final String DLQ_HEADER_EXCEPTION_CLASS = DLQ_HEADER_PREFIX + "exception.class.name";
    private static  final String DLQ_HEADER_EXCEPTION_MESSAGE = DLQ_HEADER_PREFIX + "exception.message";

    private final KafkaProducer<byte[], byte[]> producer;
    private final String dlqTopicName;
    private final String appName;

    @Override
    public DeserializationHandlerResponse handle(ConsumerRecord<byte[], byte[]> record, Exception exception) {
        logger.warn("Exception caught during Deserialization, topic: {}, partition: {}, offset: {}",
                record.topic(),
                record.partition(),
                record.offset(),
                exception);

        try {
            Headers headers = record.headers();
            addHeader(headers, DLQ_HEADER_APP_NAME, appName);
            addHeader(headers, DLQ_HEADER_TOPIC,record.topic());
            addHeader(headers, DLQ_HEADER_PARTITION,record.partition());
            addHeader(headers, DLQ_HEADER_OFFSET,record.offset());
            addHeader(headers, DLQ_HEADER_TIMESTAMP, Instant.now().toString());
            addHeader(headers, DLQ_HEADER_EXCEPTION_CLASS, exception.getClass().getCanonicalName());
            addHeader(headers, DLQ_HEADER_EXCEPTION_MESSAGE, exception.getMessage());
            producer.send(new ProducerRecord<>(dlqTopicName, null, record.timestamp(), record.key(), record.value(), headers)).get();
        } catch (Exception e) {
            logger.error("Could not send to dlq, topic: {}, partition: {}, offset: {}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    e);
        }

        return DeserializationHandlerResponse.CONTINUE;
    }

    private void addHeader(Headers headers, String headerName, String value) {
        headers.remove(headerName);
        byte[] valueAsBytes = value != null ?  value.getBytes() : null;
        headers.add(headerName, valueAsBytes);
    }

    private void addHeader(Headers headers, String headerName, Number value) {
        headers.remove(headerName);
        byte[] valueAsBytes = value != null ?  value.toString().getBytes() : null;
        headers.add(headerName, valueAsBytes);
    }

    @Override
    public void close() {
        logger.info("Closing dlq producer");
        producer.close();
    }
}
