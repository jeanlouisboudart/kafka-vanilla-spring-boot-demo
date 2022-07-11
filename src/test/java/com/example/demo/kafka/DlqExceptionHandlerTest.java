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
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static com.example.demo.kafka.DlqUtils.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class DlqExceptionHandlerTest extends BaseExceptionHandlerTest {

    @Mock
    private OnValidRecordListener<String, String> onValidRecordListener;
    @Mock
    private OnSkippedRecordListener<String, String> onSkippedRecordListener;
    @Mock
    private OnFatalErrorListener<String, String> onFatalErrorListener;

    private final MockProducer<byte[], byte[]> mockProducer = new MockProducer<>(true, new ByteArraySerializer(), new ByteArraySerializer());
    private final DlqExceptionHandler<String, String> exceptionHandler = new DlqExceptionHandler<>(mockProducer, DLQ_TOPIC, APP_NAME);

    private static final String DLQ_TOPIC = TOPIC + "-dlq";

    public DlqExceptionHandlerTest() {
        setExceptionHandler(exceptionHandler);
    }


    @Test
    @Override
    public void messageWithKeyAndValueIsValid() {
        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> record = setupMessageWithKeyAndValueIsValid(onValidRecordListener, onSkippedRecordListener, onFatalErrorListener);
        verify(onValidRecordListener).onValidRecordEvent(record);
        verify(onSkippedRecordListener, Mockito.never()).onSkippedRecordEvent(Mockito.any(), Mockito.any(), Mockito.any(Exception.class));
        verify(onFatalErrorListener, Mockito.never()).onFatalErrorEvent(Mockito.any(), Mockito.any(), Mockito.any(Exception.class));
        assertThat(mockProducer.history()).isEmpty();

    }

    @Test
    @Override
    public void messageWithoutKeyIsValid() {
        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> record = setupMessageWithoutKeyIsValid(onValidRecordListener, onSkippedRecordListener, onFatalErrorListener);
        verify(onValidRecordListener).onValidRecordEvent(record);
        verify(onSkippedRecordListener, Mockito.never()).onSkippedRecordEvent(Mockito.any(), Mockito.any(), Mockito.any());
        verify(onFatalErrorListener, Mockito.never()).onFatalErrorEvent(Mockito.any(), Mockito.any(), Mockito.any());
        assertThat(mockProducer.history()).isEmpty();
    }

    @Test
    @Override
    public void tombstoneIsValid() {
        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> record = setupTombstoneIsValid(onValidRecordListener, onSkippedRecordListener, onFatalErrorListener);
        verify(onValidRecordListener).onValidRecordEvent(record);
        verify(onSkippedRecordListener, Mockito.never()).onSkippedRecordEvent(Mockito.any(), Mockito.any(), Mockito.any());
        verify(onFatalErrorListener, Mockito.never()).onFatalErrorEvent(Mockito.any(), Mockito.any(), Mockito.any());
        assertThat(mockProducer.history()).isEmpty();

    }


    @Test
    @Override
    public void serializationErrorOnKey() {
        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> record = setupSerializationErrorOnKey(onValidRecordListener, onSkippedRecordListener, onFatalErrorListener);
        verify(onValidRecordListener, Mockito.never()).onValidRecordEvent(record);
        verify(onSkippedRecordListener).onSkippedRecordEvent(Mockito.eq(ErrorType.DESERIALIZATION_ERROR), Mockito.eq(record), Mockito.any());
        verify(onFatalErrorListener, Mockito.never()).onFatalErrorEvent(Mockito.any(), Mockito.any(), Mockito.any());

        assertThat(mockProducer.history()).hasSize(1);
        ProducerRecord<byte[], byte[]> producerRecord = mockProducer.history().iterator().next();
        assertThat(getHeaderAsString(producerRecord.headers(), DLQ_HEADER_APP_NAME)).isEqualTo(APP_NAME);
        assertThat(getHeaderAsString(producerRecord.headers(), DLQ_HEADER_TOPIC)).isEqualTo(TOPIC);
        assertThat(getHeaderAsInt(producerRecord.headers(), DLQ_HEADER_PARTITION)).isEqualTo(0);
        assertThat(getHeaderAsInt(producerRecord.headers(), DLQ_HEADER_OFFSET)).isEqualTo(1);
        assertThat(getHeaderAsString(producerRecord.headers(), DLQ_HEADER_EXCEPTION_CLASS)).isEqualTo(SerializationException.class.getCanonicalName());
        assertThat(getHeaderAsString(producerRecord.headers(), DLQ_HEADER_EXCEPTION_MESSAGE)).isEqualTo("BOOM");
        assertThat(getHeaderAsString(producerRecord.headers(), DLQ_HEADER_ERROR_TYPE)).isEqualTo(ErrorType.DESERIALIZATION_ERROR.toString());
        assertThat(producerRecord.key()).isEqualTo(POISON_PILL_KEY.getBytes());
        assertThat(producerRecord.value()).isEqualTo(VALID_VALUE.getBytes());
    }

    @Test
    @Override
    public void deserializationErrorOnValue() {
        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> record = setupDeserializationErrorOnValue(onValidRecordListener, onSkippedRecordListener, onFatalErrorListener);
        verify(onValidRecordListener, Mockito.never()).onValidRecordEvent(record);
        verify(onSkippedRecordListener).onSkippedRecordEvent(Mockito.eq(ErrorType.DESERIALIZATION_ERROR), Mockito.eq(record), Mockito.any());
        verify(onFatalErrorListener, Mockito.never()).onFatalErrorEvent(Mockito.any(), Mockito.any(), Mockito.any());

        assertThat(mockProducer.history()).hasSize(1);
        ProducerRecord<byte[], byte[]> producerRecord = mockProducer.history().iterator().next();
        assertThat(getHeaderAsString(producerRecord.headers(), DLQ_HEADER_APP_NAME)).isEqualTo(APP_NAME);
        assertThat(getHeaderAsString(producerRecord.headers(), DLQ_HEADER_TOPIC)).isEqualTo(TOPIC);
        assertThat(getHeaderAsInt(producerRecord.headers(), DLQ_HEADER_PARTITION)).isEqualTo(0);
        assertThat(getHeaderAsInt(producerRecord.headers(), DLQ_HEADER_OFFSET)).isEqualTo(1);
        assertThat(getHeaderAsString(producerRecord.headers(), DLQ_HEADER_EXCEPTION_CLASS)).isEqualTo(SerializationException.class.getCanonicalName());
        assertThat(getHeaderAsString(producerRecord.headers(), DLQ_HEADER_EXCEPTION_MESSAGE)).isEqualTo("BOOM");
        assertThat(getHeaderAsString(producerRecord.headers(), DLQ_HEADER_ERROR_TYPE)).isEqualTo(ErrorType.DESERIALIZATION_ERROR.toString());
        assertThat(producerRecord.key()).isEqualTo(VALID_KEY.getBytes());
        assertThat(producerRecord.value()).isEqualTo(POISON_PILL_VALUE.getBytes());

    }

    @Test
    @Override
    public void processingError() {
        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> record = setupProcessingError(onSkippedRecordListener, onFatalErrorListener);
        verify(onSkippedRecordListener).onSkippedRecordEvent(Mockito.eq(ErrorType.PROCESSING_ERROR), Mockito.eq(record), Mockito.any());
        verify(onFatalErrorListener, Mockito.never()).onFatalErrorEvent(Mockito.any(), Mockito.any(), Mockito.any());

        assertThat(mockProducer.history()).hasSize(1);
        ProducerRecord<byte[], byte[]> producerRecord = mockProducer.history().iterator().next();
        assertThat(getHeaderAsString(producerRecord.headers(), DLQ_HEADER_APP_NAME)).isEqualTo(APP_NAME);
        assertThat(getHeaderAsString(producerRecord.headers(), DLQ_HEADER_TOPIC)).isEqualTo(TOPIC);
        assertThat(getHeaderAsInt(producerRecord.headers(), DLQ_HEADER_PARTITION)).isEqualTo(0);
        assertThat(getHeaderAsInt(producerRecord.headers(), DLQ_HEADER_OFFSET)).isEqualTo(1);
        assertThat(getHeaderAsString(producerRecord.headers(), DLQ_HEADER_EXCEPTION_CLASS)).isEqualTo(Exception.class.getCanonicalName());
        assertThat(getHeaderAsString(producerRecord.headers(), DLQ_HEADER_EXCEPTION_MESSAGE)).isEqualTo("BOOM");
        assertThat(getHeaderAsString(producerRecord.headers(), DLQ_HEADER_ERROR_TYPE)).isEqualTo(ErrorType.PROCESSING_ERROR.toString());
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
        exceptionHandler.handleDeserializationError(fetchedRecord, onValidRecordListener, onSkippedRecordListener, onFatalErrorListener);
        verify(onValidRecordListener, Mockito.never()).onValidRecordEvent(record);
        verify(onSkippedRecordListener, Mockito.never()).onSkippedRecordEvent(Mockito.any(), Mockito.any(), Mockito.any());
        verify(onFatalErrorListener).onFatalErrorEvent(Mockito.eq(ErrorType.DESERIALIZATION_ERROR), Mockito.eq(record), Mockito.any());
        assertThat(mockProducer.history()).isEmpty();
    }

    @Override
    @BeforeEach
    public void setup() {
        super.setup();
        mockProducer.clear();
    }

}
