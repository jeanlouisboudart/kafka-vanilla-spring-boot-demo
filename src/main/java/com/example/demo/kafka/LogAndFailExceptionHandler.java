package com.example.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogAndFailExceptionHandler implements DeserializationExceptionHandler {
    private final Logger logger = LoggerFactory.getLogger(LogAndFailExceptionHandler.class);

    @Override
    public <K,V> DeserializationHandlerResponse handleProcessingError(ConsumerRecord<K,V> record, Exception exception) {
        logger.warn("Exception caught during processing, topic: {}, partition: {}, offset: {}",
                record.topic(),
                record.partition(),
                record.offset(),
                exception);
        return DeserializationHandlerResponse.FAIL;
    }

    @Override
    public DeserializationHandlerResponse handleDeserializationError(ConsumerRecord<byte[], byte[]> record, Exception exception) {
        logger.warn("Exception caught during Deserialization, topic: {}, partition: {}, offset: {}",
                record.topic(),
                record.partition(),
                record.offset(),
                exception);
        return DeserializationHandlerResponse.FAIL;
    }
}
