package com.example.demo.kafka;

import com.example.demo.services.PaymentReceiver;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class LogAndContinueExceptionHandler implements DeserializationExceptionHandler {
    private final Logger logger = LoggerFactory.getLogger(LogAndContinueExceptionHandler.class);

    @Override
    public DeserializationHandlerResponse handle(ConsumerRecord<byte[], byte[]> record, Exception exception) {
        logger.warn("Exception caught during Deserialization, topic: {}, partition: {}, offset: {}",
                record.topic(),
                record.partition(),
                record.offset(),
                exception);
        return DeserializationHandlerResponse.CONTINUE;
    }
}
