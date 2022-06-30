package com.example.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogAndContinueExceptionHandler implements KafkaExceptionHandler {
    private final Logger logger = LoggerFactory.getLogger(LogAndContinueExceptionHandler.class);

    @Override
    public <K, V> DeserializationHandlerResponse handleProcessingError(ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record, Exception exception) {
        logger.warn("Exception caught during processing, topic: {}, partition: {}, offset: {}",
                record.topic(),
                record.partition(),
                record.offset(),
                exception);
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
            return DeserializationHandlerResponse.IGNORE;
        }

        if (record.value() != null && !record.value().valid()) {
            logger.warn("Exception caught during Deserialization of the value, topic: {}, partition: {}, offset: {}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.value().getException());
            return DeserializationHandlerResponse.IGNORE;
        }
        return DeserializationHandlerResponse.VALID;
    }
}
