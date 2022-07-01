package com.example.demo.kafka;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.verify;

public class LogAndContinueExceptionHandlerTest extends BaseExceptionHandlerTest {

    private final KafkaExceptionHandler.OnValidRecord onValidRecord = Mockito.mock(KafkaExceptionHandler.OnValidRecord.class);
    private final KafkaExceptionHandler.OnSkippedRecord onSkippedRecord = Mockito.mock(KafkaExceptionHandler.OnSkippedRecord.class);
    private final KafkaExceptionHandler.OnFatalError onFatalError = Mockito.mock(KafkaExceptionHandler.OnFatalError.class);

    public LogAndContinueExceptionHandlerTest() {
        setExceptionHandler(new LogAndContinueExceptionHandler());
    }


    @Test
    @Override
    public void messageWithKeyAndValueIsValid() {
        setupMessageWithKeyAndValueIsValid(onValidRecord, onSkippedRecord, onFatalError);
        verify(onValidRecord).handle();
        verify(onSkippedRecord, Mockito.never()).handle(Mockito.any());
        verify(onFatalError, Mockito.never()).handle(Mockito.any());
    }

    @Test
    @Override
    public void messageWithoutKeyIsValid() {
        setupMessageWithoutKeyIsValid(onValidRecord, onSkippedRecord, onFatalError);
        verify(onValidRecord).handle();
        verify(onSkippedRecord, Mockito.never()).handle(Mockito.any());
        verify(onFatalError, Mockito.never()).handle(Mockito.any());
    }

    @Test
    @Override
    public void tombstoneIsValid() {
        setupTombstoneIsValid(onValidRecord, onSkippedRecord, onFatalError);
        verify(onValidRecord).handle();
        verify(onSkippedRecord, Mockito.never()).handle(Mockito.any());
        verify(onFatalError, Mockito.never()).handle(Mockito.any());
    }

    @Test
    @Override
    public void serializationErrorOnKey() {
        setupSerializationErrorOnKey(onValidRecord, onSkippedRecord, onFatalError);
        verify(onValidRecord, Mockito.never()).handle();
        verify(onSkippedRecord).handle(Mockito.any());
        verify(onFatalError, Mockito.never()).handle(Mockito.any());
    }


    @Test
    @Override
    public void deserializationErrorOnValue() {
        setupDeserializationErrorOnValue(onValidRecord, onSkippedRecord, onFatalError);
        verify(onValidRecord, Mockito.never()).handle();
        verify(onSkippedRecord).handle(Mockito.any());
        verify(onFatalError, Mockito.never()).handle(Mockito.any());
    }

    @Test
    @Override
    public void processingError() {
        setupProcessingError(onSkippedRecord, onFatalError);
        verify(onSkippedRecord).handle(Mockito.any());
        verify(onFatalError, Mockito.never()).handle(Mockito.any());
    }
}