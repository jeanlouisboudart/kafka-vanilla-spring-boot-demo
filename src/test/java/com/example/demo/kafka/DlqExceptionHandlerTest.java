package com.example.demo.kafka;

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
import org.mockito.Mockito;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static com.example.demo.kafka.DlqUtils.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

public class DlqExceptionHandlerTest extends BaseExceptionHandlerTest {

    private final KafkaExceptionHandler.OnValidRecord onValidRecord = Mockito.mock(KafkaExceptionHandler.OnValidRecord.class);
    private final KafkaExceptionHandler.OnSkippedRecord onSkippedRecord = Mockito.mock(KafkaExceptionHandler.OnSkippedRecord.class);
    private final KafkaExceptionHandler.OnFatalError onFatalError = Mockito.mock(KafkaExceptionHandler.OnFatalError.class);

    private final MockProducer<byte[], byte[]> mockProducer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
    private final DlqExceptionHandler exceptionHandler = new DlqExceptionHandler(mockProducer, DLQ_TOPIC, APP_NAME);

    private static final String DLQ_TOPIC = TOPIC + "-dlq";

    public DlqExceptionHandlerTest() {
        setExceptionHandler(exceptionHandler);
    }


    @Test
    @Override
    public void messageWithKeyAndValueIsValid() {
        setupMessageWithKeyAndValueIsValid(onValidRecord, onSkippedRecord, onFatalError);
        verify(onValidRecord).handle();
        verify(onSkippedRecord, Mockito.never()).handle(Mockito.any(Exception.class));
        verify(onFatalError, Mockito.never()).handle(Mockito.any(Exception.class));
        assertThat(mockProducer.history()).isEmpty();

    }

    @Test
    @Override
    public void messageWithoutKeyIsValid() {
        setupMessageWithoutKeyIsValid(onValidRecord, onSkippedRecord, onFatalError);
        verify(onValidRecord).handle();
        verify(onSkippedRecord, Mockito.never()).handle(Mockito.any());
        verify(onFatalError, Mockito.never()).handle(Mockito.any());
        assertThat(mockProducer.history()).isEmpty();
    }

    @Test
    @Override
    public void tombstoneIsValid() {
        setupTombstoneIsValid(onValidRecord, onSkippedRecord, onFatalError);
        verify(onValidRecord).handle();
        verify(onSkippedRecord, Mockito.never()).handle(Mockito.any());
        verify(onFatalError, Mockito.never()).handle(Mockito.any());
        assertThat(mockProducer.history()).isEmpty();

    }


    @Test
    @Override
    public void serializationErrorOnKey() {
        setupSerializationErrorOnKey(onValidRecord, onSkippedRecord, onFatalError);
        verify(onValidRecord, Mockito.never()).handle();
        verify(onSkippedRecord).handle(Mockito.any());
        verify(onFatalError, Mockito.never()).handle(Mockito.any());

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
        setupDeserializationErrorOnValue(onValidRecord, onSkippedRecord, onFatalError);
        verify(onValidRecord, Mockito.never()).handle();
        verify(onSkippedRecord).handle(Mockito.any());
        verify(onFatalError, Mockito.never()).handle(Mockito.any());

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
        setupProcessingError(onSkippedRecord, onFatalError);
        verify(onSkippedRecord).handle(Mockito.any());
        verify(onFatalError, Mockito.never()).handle(Mockito.any());

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
        exceptionHandler.handleDeserializationError(fetchedRecord, onValidRecord, onSkippedRecord, onFatalError);
        verify(onValidRecord, Mockito.never()).handle();
        verify(onSkippedRecord, Mockito.never()).handle(Mockito.any());
        verify(onFatalError).handle(Mockito.any());
        assertThat(mockProducer.history()).isEmpty();
    }

    @Override
    @BeforeEach
    public void setup() {
        super.setup();
        mockProducer.clear();
    }

}
