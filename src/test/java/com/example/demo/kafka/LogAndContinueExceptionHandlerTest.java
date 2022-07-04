package com.example.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.verify;

public class LogAndContinueExceptionHandlerTest extends BaseExceptionHandlerTest {

    private final KafkaExceptionHandler.OnValidRecordListener onValidRecordListener = Mockito.mock(KafkaExceptionHandler.OnValidRecordListener.class);
    private final KafkaExceptionHandler.OnSkippedRecordListener onSkippedRecordListener = Mockito.mock(KafkaExceptionHandler.OnSkippedRecordListener.class);
    private final KafkaExceptionHandler.OnFatalErrorListener onFatalErrorListener = Mockito.mock(KafkaExceptionHandler.OnFatalErrorListener.class);

    public LogAndContinueExceptionHandlerTest() {
        setExceptionHandler(new LogAndContinueExceptionHandler());
    }


    @Test
    @Override
    public void messageWithKeyAndValueIsValid() {
        setupMessageWithKeyAndValueIsValid(onValidRecordListener, onSkippedRecordListener, onFatalErrorListener);
        verify(onValidRecordListener).onValidRecordEvent();
        verify(onSkippedRecordListener, Mockito.never()).onSkippedRecordEvent(Mockito.any(), Mockito.any(), Mockito.any());
        verify(onFatalErrorListener, Mockito.never()).onFatalErrorEvent(Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    @Override
    public void messageWithoutKeyIsValid() {
        setupMessageWithoutKeyIsValid(onValidRecordListener, onSkippedRecordListener, onFatalErrorListener);
        verify(onValidRecordListener).onValidRecordEvent();
        verify(onSkippedRecordListener, Mockito.never()).onSkippedRecordEvent(Mockito.any(), Mockito.any(), Mockito.any());
        verify(onFatalErrorListener, Mockito.never()).onFatalErrorEvent(Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    @Override
    public void tombstoneIsValid() {
        setupTombstoneIsValid(onValidRecordListener, onSkippedRecordListener, onFatalErrorListener);
        verify(onValidRecordListener).onValidRecordEvent();
        verify(onSkippedRecordListener, Mockito.never()).onSkippedRecordEvent(Mockito.any(), Mockito.any(), Mockito.any());
        verify(onFatalErrorListener, Mockito.never()).onFatalErrorEvent(Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    @Override
    public void serializationErrorOnKey() {
        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> record = setupSerializationErrorOnKey(onValidRecordListener, onSkippedRecordListener, onFatalErrorListener);
        verify(onValidRecordListener, Mockito.never()).onValidRecordEvent();
        verify(onSkippedRecordListener).onSkippedRecordEvent(Mockito.eq(KafkaExceptionHandler.ErrorType.DESERIALIZATION_ERROR), Mockito.eq(record), Mockito.any());
        verify(onFatalErrorListener, Mockito.never()).onFatalErrorEvent(Mockito.any(), Mockito.any(), Mockito.any());
    }


    @Test
    @Override
    public void deserializationErrorOnValue() {
        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> record = setupDeserializationErrorOnValue(onValidRecordListener, onSkippedRecordListener, onFatalErrorListener);
        verify(onValidRecordListener, Mockito.never()).onValidRecordEvent();
        verify(onSkippedRecordListener).onSkippedRecordEvent(Mockito.eq(KafkaExceptionHandler.ErrorType.DESERIALIZATION_ERROR), Mockito.eq(record), Mockito.any());
        verify(onFatalErrorListener, Mockito.never()).onFatalErrorEvent(Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    @Override
    public void processingError() {
        ConsumerRecord<DeserializerResult<String>, DeserializerResult<String>> record = setupProcessingError(onSkippedRecordListener, onFatalErrorListener);
        verify(onSkippedRecordListener).onSkippedRecordEvent(Mockito.eq(KafkaExceptionHandler.ErrorType.PROCESSING_ERROR), Mockito.eq(record), Mockito.any());
        verify(onFatalErrorListener, Mockito.never()).onFatalErrorEvent(Mockito.any(), Mockito.any(), Mockito.any());
    }
}