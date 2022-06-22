package com.example.demo.kafka;

import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

@AllArgsConstructor
public class DlqExceptionHandler implements DeserializationExceptionHandler, Closeable {
    private final Logger logger = LoggerFactory.getLogger(DlqExceptionHandler.class);

    private final KafkaProducer<byte[], byte[]> producer;
    private final String dlqTopicName;

    @Override
    public DeserializationHandlerResponse handle(ConsumerRecord<byte[], byte[]> record, Exception exception) {
        logger.warn("Exception caught during Deserialization, topic: {}, partition: {}, offset: {}",
                record.topic(),
                record.partition(),
                record.offset(),
                exception);

        try {
            //TODO: capture headers
            Headers headers = record.headers();
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

    @Override
    public void close() {
        logger.info("Closing dlq producer");
        producer.close();
    }
}
