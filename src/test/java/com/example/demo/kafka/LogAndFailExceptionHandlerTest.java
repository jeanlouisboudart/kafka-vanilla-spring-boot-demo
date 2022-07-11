package com.example.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class LogAndFailExceptionHandlerTest extends BaseExceptionHandlerTest {

    @Mock
    private OnValidRecordListener<String, String> onValidRecordListener;
    @Mock
    private OnSkippedRecordListener<String, String> onSkippedRecordListener;
    @Mock
    private OnFatalErrorListener<String, String> onFatalErrorListener;

    public LogAndFailExceptionHandlerTest() {
        setExceptionHandler(new LogAndFailExceptionHandler<>());
    }

    @Override
    @Test
    public void messageWithKeyAndValueIsValid() {
        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> record = setupMessageWithKeyAndValueIsValid(onValidRecordListener, onSkippedRecordListener, onFatalErrorListener);
        verify(onValidRecordListener).onValidRecordEvent(record);
        verify(onSkippedRecordListener, Mockito.never()).onSkippedRecordEvent(Mockito.any(), Mockito.any(), Mockito.any());
        verify(onFatalErrorListener, Mockito.never()).onFatalErrorEvent(Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    @Override
    public void messageWithoutKeyIsValid() {
        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> record = setupMessageWithoutKeyIsValid(onValidRecordListener, onSkippedRecordListener, onFatalErrorListener);
        verify(onValidRecordListener).onValidRecordEvent(record);
        verify(onSkippedRecordListener, Mockito.never()).onSkippedRecordEvent(Mockito.any(), Mockito.any(), Mockito.any());
        verify(onFatalErrorListener, Mockito.never()).onFatalErrorEvent(Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    @Override
    public void tombstoneIsValid() {
        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> record = setupTombstoneIsValid(onValidRecordListener, onSkippedRecordListener, onFatalErrorListener);
        verify(onValidRecordListener).onValidRecordEvent(record);
        verify(onSkippedRecordListener, Mockito.never()).onSkippedRecordEvent(Mockito.any(), Mockito.any(), Mockito.any());
        verify(onFatalErrorListener, Mockito.never()).onFatalErrorEvent(Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    @Override
    public void serializationErrorOnKey() {
        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> record = setupSerializationErrorOnKey(onValidRecordListener, onSkippedRecordListener, onFatalErrorListener);
        verify(onValidRecordListener, Mockito.never()).onValidRecordEvent(record);
        verify(onSkippedRecordListener, Mockito.never()).onSkippedRecordEvent(Mockito.any(), Mockito.any(), Mockito.any());
        verify(onFatalErrorListener).onFatalErrorEvent(Mockito.eq(ErrorType.DESERIALIZATION_ERROR), Mockito.eq(record), Mockito.any());
    }

    @Test
    @Override
    public void deserializationErrorOnValue() {
        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> record = setupDeserializationErrorOnValue(onValidRecordListener, onSkippedRecordListener, onFatalErrorListener);
        verify(onValidRecordListener, Mockito.never()).onValidRecordEvent(record);
        verify(onSkippedRecordListener, Mockito.never()).onSkippedRecordEvent(Mockito.any(), Mockito.any(), Mockito.any());
        verify(onFatalErrorListener).onFatalErrorEvent(Mockito.eq(ErrorType.DESERIALIZATION_ERROR), Mockito.eq(record), Mockito.any());
    }

    @Test
    @Override
    public void processingError() {
        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> record = setupProcessingError(onSkippedRecordListener, onFatalErrorListener);
        verify(onSkippedRecordListener, Mockito.never()).onSkippedRecordEvent(Mockito.any(), Mockito.any(), Mockito.any());
        verify(onFatalErrorListener).onFatalErrorEvent(Mockito.eq(ErrorType.PROCESSING_ERROR), Mockito.eq(record), Mockito.any());
    }
}