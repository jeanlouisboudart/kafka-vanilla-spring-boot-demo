package com.example.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.example.demo.kafka.DlqUtils.*;
import static org.assertj.core.api.Assertions.assertThat;

public class DlqExceptionHandlerTest {

    private final MockConsumer<DeserializerResult<String>, DeserializerResult<String>> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    private final MockProducer<byte[], byte[]> mockProducer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
    private final DlqExceptionHandler dlqExceptionHandler = new DlqExceptionHandler(mockProducer, DLQ_TOPIC, APP_NAME);
    private static final String TOPIC = "mytopic";
    private static final String DLQ_TOPIC = TOPIC + "-dlq";

    private static final String APP_NAME = "myapp";
    private static final String VALID_KEY = "key";
    private static final String VALID_VALUE = "value";
    private static final String POISON_PILL_KEY = "poison-key";
    private static final String POISON_PILL_VALUE = "poison-value";

    @Test
    void messageWithKeyAndValueIsValid() {
        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> record = new ConsumerRecord<>(
                TOPIC,
                0,
                1,
                Instant.now().toEpochMilli(),
                TimestampType.LOG_APPEND_TIME,
                0,
                0,
                new DeserializerResult<>(VALID_KEY, VALID_KEY.getBytes()),
                new DeserializerResult<>(VALID_VALUE, VALID_VALUE.getBytes()),
                new RecordHeaders(),
                Optional.empty());

        mockConsumer.addRecord(record);
        ConsumerRecords<DeserializerResult<String>, DeserializerResult<String>> records = mockConsumer.poll(Duration.ofSeconds(1));

        assertThat(records).isNotEmpty();
        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> fetchedRecord = records.iterator().next();

        KafkaExceptionHandler.DeserializationHandlerResponse handlerResponse = dlqExceptionHandler.handleDeserializationError(fetchedRecord);
        assertThat(handlerResponse).isEqualTo(KafkaExceptionHandler.DeserializationHandlerResponse.VALID);
        assertThat(mockProducer.history()).isEmpty();
    }

    @Test
    void messageWithoutKeyIsValid() {
        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> record = new ConsumerRecord<>(
                TOPIC,
                0,
                1,
                Instant.now().toEpochMilli(),
                TimestampType.LOG_APPEND_TIME,
                0,
                0,
                new DeserializerResult<>(),
                new DeserializerResult<>(VALID_VALUE, VALID_VALUE.getBytes()),
                new RecordHeaders(),
                Optional.empty());

        mockConsumer.addRecord(record);
        ConsumerRecords<DeserializerResult<String>, DeserializerResult<String>> records = mockConsumer.poll(Duration.ofSeconds(1));

        assertThat(records).isNotEmpty();
        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> fetchedRecord = records.iterator().next();

        KafkaExceptionHandler.DeserializationHandlerResponse handlerResponse = dlqExceptionHandler.handleDeserializationError(fetchedRecord);
        assertThat(handlerResponse).isEqualTo(KafkaExceptionHandler.DeserializationHandlerResponse.VALID);
        assertThat(mockProducer.history()).isEmpty();
    }

    @Test
    void tombstoneIsValid() {
        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> record = new ConsumerRecord<>(
                TOPIC,
                0,
                1,
                Instant.now().toEpochMilli(),
                TimestampType.LOG_APPEND_TIME,
                0,
                0,
                new DeserializerResult<>(VALID_KEY, VALID_KEY.getBytes()),
                new DeserializerResult<>(),
                new RecordHeaders(),
                Optional.empty());

        mockConsumer.addRecord(record);
        ConsumerRecords<DeserializerResult<String>, DeserializerResult<String>> records = mockConsumer.poll(Duration.ofSeconds(1));

        assertThat(records).isNotEmpty();
        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> fetchedRecord = records.iterator().next();


        KafkaExceptionHandler.DeserializationHandlerResponse handlerResponse = dlqExceptionHandler.handleDeserializationError(fetchedRecord);
        assertThat(handlerResponse).isEqualTo(KafkaExceptionHandler.DeserializationHandlerResponse.VALID);
        assertThat(mockProducer.history()).isEmpty();
    }

    @Test
    void deserializationErrorOnKey() {
        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> record = new ConsumerRecord<>(
                TOPIC,
                0,
                1,
                Instant.now().toEpochMilli(),
                TimestampType.LOG_APPEND_TIME,
                0,
                0,
                new DeserializerResult<>(null, POISON_PILL_KEY.getBytes(), new SerializationException("BOOM")),
                new DeserializerResult<>(VALID_VALUE, VALID_VALUE.getBytes()),
                new RecordHeaders(),
                Optional.empty());

        mockConsumer.addRecord(record);
        ConsumerRecords<DeserializerResult<String>, DeserializerResult<String>> records = mockConsumer.poll(Duration.ofSeconds(1));

        assertThat(records).isNotEmpty();
        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> fetchedRecord = records.iterator().next();

        KafkaExceptionHandler.DeserializationHandlerResponse handlerResponse = dlqExceptionHandler.handleDeserializationError(fetchedRecord);
        assertThat(handlerResponse).isEqualTo(KafkaExceptionHandler.DeserializationHandlerResponse.IGNORE);
        assertThat(mockProducer.history()).hasSize(1);
        ProducerRecord<byte[], byte[]> producerRecord = mockProducer.history().iterator().next();
        assertThat(getHeaderAsString(producerRecord.headers(), DLQ_HEADER_APP_NAME)).isEqualTo(APP_NAME);
        assertThat(getHeaderAsString(producerRecord.headers(), DLQ_HEADER_TOPIC)).isEqualTo(TOPIC);
        assertThat(getHeaderAsInt(producerRecord.headers(), DLQ_HEADER_PARTITION)).isEqualTo(0);
        assertThat(getHeaderAsInt(producerRecord.headers(), DLQ_HEADER_OFFSET)).isEqualTo(1);
        assertThat(getHeaderAsString(producerRecord.headers(), DLQ_HEADER_EXCEPTION_CLASS)).isEqualTo(SerializationException.class.getCanonicalName());
        assertThat(getHeaderAsString(producerRecord.headers(), DLQ_HEADER_EXCEPTION_MESSAGE)).isEqualTo("BOOM");
        assertThat(producerRecord.key()).isEqualTo(POISON_PILL_KEY.getBytes());
        assertThat(producerRecord.value()).isEqualTo(VALID_VALUE.getBytes());
    }

    @Test
    void deserializationErrorOnValue() {
        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> record = new ConsumerRecord<>(
                TOPIC,
                0,
                1,
                Instant.now().toEpochMilli(),
                TimestampType.LOG_APPEND_TIME,
                0,
                0,
                new DeserializerResult<>(VALID_KEY, VALID_KEY.getBytes()),
                new DeserializerResult<>(POISON_PILL_VALUE.getBytes(), new SerializationException("BOOM")),
                new RecordHeaders(),
                Optional.empty());

        mockConsumer.addRecord(record);
        ConsumerRecords<DeserializerResult<String>, DeserializerResult<String>> records = mockConsumer.poll(Duration.ofSeconds(1));

        assertThat(records).isNotEmpty();
        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> fetchedRecord = records.iterator().next();

        KafkaExceptionHandler.DeserializationHandlerResponse handlerResponse = dlqExceptionHandler.handleDeserializationError(fetchedRecord);
        assertThat(handlerResponse).isEqualTo(KafkaExceptionHandler.DeserializationHandlerResponse.IGNORE);
        assertThat(mockProducer.history()).hasSize(1);
        ProducerRecord<byte[], byte[]> producerRecord = mockProducer.history().iterator().next();
        assertThat(getHeaderAsString(producerRecord.headers(), DLQ_HEADER_APP_NAME)).isEqualTo(APP_NAME);
        assertThat(getHeaderAsString(producerRecord.headers(), DLQ_HEADER_TOPIC)).isEqualTo(TOPIC);
        assertThat(getHeaderAsInt(producerRecord.headers(), DLQ_HEADER_PARTITION)).isEqualTo(0);
        assertThat(getHeaderAsInt(producerRecord.headers(), DLQ_HEADER_OFFSET)).isEqualTo(1);
        assertThat(getHeaderAsString(producerRecord.headers(), DLQ_HEADER_EXCEPTION_CLASS)).isEqualTo(SerializationException.class.getCanonicalName());
        assertThat(getHeaderAsString(producerRecord.headers(), DLQ_HEADER_EXCEPTION_MESSAGE)).isEqualTo("BOOM");
        assertThat(producerRecord.key()).isEqualTo(VALID_KEY.getBytes());
        assertThat(producerRecord.value()).isEqualTo(POISON_PILL_VALUE.getBytes());
    }

    @Test
    void failToWriteToDLQ() {
        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> record = new ConsumerRecord<>(
                TOPIC,
                0,
                1,
                Instant.now().toEpochMilli(),
                TimestampType.LOG_APPEND_TIME,
                0,
                0,
                new DeserializerResult<>(VALID_KEY, VALID_KEY.getBytes()),
                new DeserializerResult<>(POISON_PILL_VALUE.getBytes(), new SerializationException("BOOM")),
                new RecordHeaders(),
                Optional.empty());

        mockConsumer.addRecord(record);
        ConsumerRecords<DeserializerResult<String>, DeserializerResult<String>> records = mockConsumer.poll(Duration.ofSeconds(1));

        assertThat(records).isNotEmpty();
        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> fetchedRecord = records.iterator().next();

        mockProducer.sendException = new TimeoutException();
        KafkaExceptionHandler.DeserializationHandlerResponse handlerResponse = dlqExceptionHandler.handleDeserializationError(fetchedRecord);
        assertThat(handlerResponse).isEqualTo(KafkaExceptionHandler.DeserializationHandlerResponse.IGNORE);
        assertThat(mockProducer.history()).isEmpty();
    }


    @BeforeEach
    public void setup() {
        mockConsumer.subscribe(Collections.singleton(TOPIC));
        mockConsumer.rebalance(Collections.singleton(new TopicPartition(TOPIC, 0)));

        Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(new TopicPartition(TOPIC, 0), 0L);
        mockConsumer.updateBeginningOffsets(beginningOffsets);

        mockProducer.clear();
    }

}
