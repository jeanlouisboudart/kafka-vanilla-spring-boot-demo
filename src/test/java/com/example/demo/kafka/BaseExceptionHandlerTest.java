package com.example.demo.kafka;

import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseExceptionHandlerTest {
    protected static final String TOPIC = "mytopic";
    protected static final String APP_NAME = "myapp";
    protected static final String VALID_KEY = "key";
    protected static final String POISON_PILL_VALUE = "poison-value";
    protected static final String VALID_VALUE = "value";
    protected static final String POISON_PILL_KEY = "poison-key";
    protected final MockConsumer<DeserializerResult<String>, DeserializerResult<String>> mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

    @Setter
    private KafkaExceptionHandler exceptionHandler;

    public abstract void messageWithKeyAndValueIsValid();

    protected KafkaExceptionHandler.DeserializationHandlerResponse setupMessageWithKeyAndValueIsValid() {

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

        assertThat(records).

                isNotEmpty();

        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> fetchedRecord = records.iterator().next();

        return exceptionHandler.handleDeserializationError(fetchedRecord);
    }

    public abstract void messageWithoutKeyIsValid();

    protected KafkaExceptionHandler.DeserializationHandlerResponse setupMessageWithoutKeyIsValid() {
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

        return exceptionHandler.handleDeserializationError(fetchedRecord);
    }

    public abstract void tombstoneIsValid();

    protected KafkaExceptionHandler.DeserializationHandlerResponse setupTombstoneIsValid() {
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

        return exceptionHandler.handleDeserializationError(fetchedRecord);
    }

    public abstract void serializationErrorOnKey();

    protected KafkaExceptionHandler.DeserializationHandlerResponse setupSerializationErrorOnKey() {
        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> record = new ConsumerRecord<>(
                TOPIC,
                0,
                1,
                Instant.now().toEpochMilli(),
                TimestampType.LOG_APPEND_TIME,
                0,
                0,
                new DeserializerResult<>(POISON_PILL_KEY.getBytes(), new SerializationException("BOOM")),
                new DeserializerResult<>(VALID_VALUE, VALID_VALUE.getBytes()),
                new RecordHeaders(),
                Optional.empty());

        mockConsumer.addRecord(record);
        ConsumerRecords<DeserializerResult<String>, DeserializerResult<String>> records = mockConsumer.poll(Duration.ofSeconds(1));

        assertThat(records).isNotEmpty();
        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> fetchedRecord = records.iterator().next();

        return exceptionHandler.handleDeserializationError(fetchedRecord);
    }

    public abstract void deserializationErrorOnValue();

    protected KafkaExceptionHandler.DeserializationHandlerResponse setupDeserializationErrorOnValue() {
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

        return exceptionHandler.handleDeserializationError(fetchedRecord);
    }

    public abstract void processingError();

    protected KafkaExceptionHandler.DeserializationHandlerResponse setupProcessingError() {
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

        return exceptionHandler.handleProcessingError(fetchedRecord, new Exception("BOOM"));
    }

    @BeforeEach
    public void setup() {
        mockConsumer.subscribe(Collections.singleton(TOPIC));
        mockConsumer.rebalance(Collections.singleton(new TopicPartition(TOPIC, 0)));

        Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(new TopicPartition(TOPIC, 0), 0L);
        mockConsumer.updateBeginningOffsets(beginningOffsets);
    }
}
