package com.example.demo.kafka;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.verify;

public class LogAndFailExceptionHandlerTest extends BaseExceptionHandlerTest {

    private final KafkaExceptionHandler.OnValidRecordListener onValidRecordListener = Mockito.mock(KafkaExceptionHandler.OnValidRecordListener.class);
    private final KafkaExceptionHandler.OnSkippedRecordListener onSkippedRecordListener = Mockito.mock(KafkaExceptionHandler.OnSkippedRecordListener.class);
    private final KafkaExceptionHandler.OnFatalErrorListener onFatalErrorListener = Mockito.mock(KafkaExceptionHandler.OnFatalErrorListener.class);

    public LogAndFailExceptionHandlerTest() {
        setExceptionHandler(new LogAndFailExceptionHandler());
    }

    @Override
    @Test
    public void messageWithKeyAndValueIsValid() {
        setupMessageWithKeyAndValueIsValid(onValidRecordListener, onSkippedRecordListener, onFatalErrorListener);
        verify(onValidRecordListener).onValidRecordEvent();
        verify(onSkippedRecordListener, Mockito.never()).onSkippedRecordEvent(Mockito.any());
        verify(onFatalErrorListener, Mockito.never()).onFatalErrorEvent(Mockito.any());
    }

    @Test
    @Override
    public void messageWithoutKeyIsValid() {
        setupMessageWithoutKeyIsValid(onValidRecordListener, onSkippedRecordListener, onFatalErrorListener);
        verify(onValidRecordListener).onValidRecordEvent();
        verify(onSkippedRecordListener, Mockito.never()).onSkippedRecordEvent(Mockito.any());
        verify(onFatalErrorListener, Mockito.never()).onFatalErrorEvent(Mockito.any());
    }

    @Test
    @Override
    public void tombstoneIsValid() {
        setupTombstoneIsValid(onValidRecordListener, onSkippedRecordListener, onFatalErrorListener);
        verify(onValidRecordListener).onValidRecordEvent();
        verify(onSkippedRecordListener, Mockito.never()).onSkippedRecordEvent(Mockito.any());
        verify(onFatalErrorListener, Mockito.never()).onFatalErrorEvent(Mockito.any());
    }

    @Test
    @Override
    public void serializationErrorOnKey() {
        setupSerializationErrorOnKey(onValidRecordListener, onSkippedRecordListener, onFatalErrorListener);
        verify(onValidRecordListener, Mockito.never()).onValidRecordEvent();
        verify(onSkippedRecordListener, Mockito.never()).onSkippedRecordEvent(Mockito.any());
        verify(onFatalErrorListener).onFatalErrorEvent(Mockito.any());
    }

    @Test
    @Override
    public void deserializationErrorOnValue() {
        setupDeserializationErrorOnValue(onValidRecordListener, onSkippedRecordListener, onFatalErrorListener);
        verify(onValidRecordListener, Mockito.never()).onValidRecordEvent();
        verify(onSkippedRecordListener, Mockito.never()).onSkippedRecordEvent(Mockito.any());
        verify(onFatalErrorListener).onFatalErrorEvent(Mockito.any());
    }

    @Test
    @Override
    public void processingError() {
        setupProcessingError(onSkippedRecordListener, onFatalErrorListener);
        verify(onSkippedRecordListener, Mockito.never()).onSkippedRecordEvent(Mockito.any());
        verify(onFatalErrorListener).onFatalErrorEvent(Mockito.any());
    }
}