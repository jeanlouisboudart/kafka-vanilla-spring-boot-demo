package com.example.demo.kafka.streams;

import com.example.demo.kafka.consumers.ErrorHandler;
import com.example.demo.config.KafkaConfig;
import com.example.demo.kafka.DlqUtils;
import com.example.demo.kafka.ErrorType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.DeserializationExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static com.example.demo.kafka.DlqUtils.*;

public class KStreamDeserializationHandler implements DeserializationExceptionHandler {

    private final Logger logger = LoggerFactory.getLogger(KStreamDeserializationHandler.class);

    private KafkaProducer<byte[], byte[]> dlqProducer;
    private String applicationId;
    private String dlqTopicName;
    private ErrorHandler errorHandlerType;
    public static final String DLQ_EXCEPTION_HANDLER_CONFIG = "dlq.exception.handler";
    public static final String DLQ_TOPIC_NAME_CONFIG = "dlq.topic.name";

    @Override
    public DeserializationHandlerResponse handle(ProcessorContext context, ConsumerRecord<byte[], byte[]> record, Exception exception) {
        switch (errorHandlerType) {
            case LogAndFail:
                logger.error("Exception caught during Deserialization, topic: " + record.topic() + ", partition: " + record.partition() + ", offset: " + record.offset(), exception);
                return DeserializationHandlerResponse.FAIL;
            case LogAndContinue:
                logger.error("Exception caught during Deserialization, topic: " + record.topic() + ", partition: " + record.partition() + ", offset: " + record.offset(), exception);
                return DeserializationHandlerResponse.CONTINUE;
            case DeadLetterQueue:
                return publishToDlq(record, exception);
            default:
                throw new IllegalStateException("unknown exception handler: " + errorHandlerType);
        }
    }

    private DeserializationHandlerResponse publishToDlq(ConsumerRecord<byte[], byte[]> record, Exception exception) {
        logger.warn("Exception caught during Deserialization, topic: {}, partition: {}, offset: {}",
                record.topic(),
                record.partition(),
                record.offset(),
                exception);
        return writeToDlqTopic(record, exception);
    }

    private DeserializationHandlerResponse writeToDlqTopic(ConsumerRecord<byte[], byte[]> record, Exception exception) {
        Headers headers = record.headers();
        addHeader(headers, DLQ_HEADER_APP_NAME, applicationId);
        addHeader(headers, DLQ_HEADER_TOPIC, record.topic());
        addHeader(headers, DLQ_HEADER_PARTITION, record.partition());
        addHeader(headers, DLQ_HEADER_OFFSET, record.offset());
        addHeader(headers, DLQ_HEADER_TIMESTAMP, Instant.now().toString());
        addHeader(headers, DLQ_HEADER_EXCEPTION_CLASS, exception.getClass().getCanonicalName());
        addHeader(headers, DLQ_HEADER_EXCEPTION_MESSAGE, exception.getMessage());
        DlqUtils.addHeader(headers, DLQ_HEADER_ERROR_TYPE, ErrorType.DESERIALIZATION_ERROR.toString());

        //Here we use synchronous send because we want to make sure we wrote to DLQ before moving forward.
        try {
            dlqProducer.send(new ProducerRecord<>(dlqTopicName, null, record.timestamp(), record.key(), record.key(), headers)).get();
            return DeserializationHandlerResponse.CONTINUE;
        } catch (Exception e) {
            logger.error("Could not send to dlq, topic: {}, partition: {}, offset: {}",
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    e);
            return DeserializationHandlerResponse.FAIL;
        }
    }

    @Override
    public void configure(Map<String, ?> configs) {
        Map<String, Object> dlqConfig = new HashMap<>(configs);
        String errorHandlerTypeAsString = (String) dlqConfig.getOrDefault(DLQ_EXCEPTION_HANDLER_CONFIG, ErrorHandler.LogAndFail.toString());
        errorHandlerType = ErrorHandler.valueOf(errorHandlerTypeAsString);
        if (errorHandlerType == ErrorHandler.DeadLetterQueue) {
            dlqConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
            dlqConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
            applicationId = (String) configs.get(StreamsConfig.APPLICATION_ID_CONFIG);
            dlqConfig.put(ProducerConfig.CLIENT_ID_CONFIG, applicationId + KafkaConfig.DLQ_SUFFIX);
            dlqProducer = new KafkaProducer<>(dlqConfig);

            dlqTopicName = (String) dlqConfig.getOrDefault(DLQ_TOPIC_NAME_CONFIG, applicationId + KafkaConfig.DLQ_SUFFIX);

        }
    }
}
