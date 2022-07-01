package com.example.demo.kafka;

import com.example.demo.kafka.KafkaExceptionHandler.DeserializationHandlerResponse;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static com.example.demo.kafka.DlqUtils.*;
import static com.example.demo.kafka.KafkaExceptionHandler.DeserializationHandlerResponse.IGNORE;
import static com.example.demo.kafka.KafkaExceptionHandler.DeserializationHandlerResponse.VALID;
import static org.assertj.core.api.Assertions.assertThat;

public class DlqExceptionHandlerTest extends BaseExceptionHandlerTest {

    private final MockProducer<byte[], byte[]> mockProducer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
    private final DlqExceptionHandler exceptionHandler = new DlqExceptionHandler(mockProducer, DLQ_TOPIC, APP_NAME);
    private static final String DLQ_TOPIC = TOPIC + "-dlq";

    public DlqExceptionHandlerTest() {
        setExceptionHandler(exceptionHandler);
    }


    @Test
    @Override
    public void messageWithKeyAndValueIsValid() {
        DeserializationHandlerResponse handlerResponse = setupMessageWithKeyAndValueIsValid();
        assertThat(handlerResponse).isEqualTo(VALID);
        assertThat(mockProducer.history()).isEmpty();

    }

    @Test
    @Override
    public void messageWithoutKeyIsValid() {
        DeserializationHandlerResponse handlerResponse = setupMessageWithoutKeyIsValid();
        assertThat(handlerResponse).isEqualTo(VALID);
        assertThat(mockProducer.history()).isEmpty();
    }

    @Test
    @Override
    public void tombstoneIsValid() {
        DeserializationHandlerResponse handlerResponse = setupTombstoneIsValid();
        assertThat(handlerResponse).isEqualTo(VALID);
        assertThat(mockProducer.history()).isEmpty();

    }


    @Test
    @Override
    public void serializationErrorOnKey() {
        DeserializationHandlerResponse handlerResponse = setupSerializationErrorOnKey();
        assertThat(handlerResponse).isEqualTo(IGNORE);

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
    @Override
    public void deserializationErrorOnValue() {
        DeserializationHandlerResponse handlerResponse = setupDeserializationErrorOnValue();
        assertThat(handlerResponse).isEqualTo(IGNORE);

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
    @Override
    public void processingError() {
        DeserializationHandlerResponse handlerResponse = setupProcessingError();
        assertThat(handlerResponse).isEqualTo(IGNORE);

        assertThat(mockProducer.history()).hasSize(1);
        ProducerRecord<byte[], byte[]> producerRecord = mockProducer.history().iterator().next();
        assertThat(getHeaderAsString(producerRecord.headers(), DLQ_HEADER_APP_NAME)).isEqualTo(APP_NAME);
        assertThat(getHeaderAsString(producerRecord.headers(), DLQ_HEADER_TOPIC)).isEqualTo(TOPIC);
        assertThat(getHeaderAsInt(producerRecord.headers(), DLQ_HEADER_PARTITION)).isEqualTo(0);
        assertThat(getHeaderAsInt(producerRecord.headers(), DLQ_HEADER_OFFSET)).isEqualTo(1);
        assertThat(getHeaderAsString(producerRecord.headers(), DLQ_HEADER_EXCEPTION_CLASS)).isEqualTo(Exception.class.getCanonicalName());
        assertThat(getHeaderAsString(producerRecord.headers(), DLQ_HEADER_EXCEPTION_MESSAGE)).isEqualTo("BOOM");
        assertThat(producerRecord.key()).isEqualTo(VALID_KEY.getBytes());
        assertThat(producerRecord.value()).isEqualTo(VALID_VALUE.getBytes());

    }

    @Test
    public void failToWriteToDLQ() {
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
        DeserializationHandlerResponse handlerResponse = exceptionHandler.handleDeserializationError(fetchedRecord);
        assertThat(handlerResponse).isEqualTo(IGNORE);
        assertThat(mockProducer.history()).isEmpty();
    }

    @Override
    @BeforeEach
    public void setup() {
        super.setup();
        mockProducer.clear();
    }

}
