package com.example.demo.kafka;

import org.junit.jupiter.api.Test;

import static com.example.demo.kafka.KafkaExceptionHandler.DeserializationHandlerResponse.IGNORE;
import static com.example.demo.kafka.KafkaExceptionHandler.DeserializationHandlerResponse.VALID;
import static org.assertj.core.api.Assertions.assertThat;

public class LogAndContinueExceptionHandlerTest extends BaseExceptionHandlerTest {

    @Test
    @Override
    public void messageWithKeyAndValueIsValid() {
        KafkaExceptionHandler.DeserializationHandlerResponse handlerResponse = setupMessageWithKeyAndValueIsValid();
        assertThat(handlerResponse).isEqualTo(VALID);
    }

    public LogAndContinueExceptionHandlerTest() {
        setExceptionHandler(new LogAndContinueExceptionHandler());
    }


    @Test
    @Override
    public void messageWithoutKeyIsValid() {
        KafkaExceptionHandler.DeserializationHandlerResponse handlerResponse = setupMessageWithoutKeyIsValid();
        assertThat(handlerResponse).isEqualTo(VALID);
    }

    @Test
    @Override
    public void tombstoneIsValid() {
        KafkaExceptionHandler.DeserializationHandlerResponse handlerResponse = setupTombstoneIsValid();
        assertThat(handlerResponse).isEqualTo(VALID);
    }

    @Test
    @Override
    public void serializationErrorOnKey() {
        KafkaExceptionHandler.DeserializationHandlerResponse handlerResponse = setupSerializationErrorOnKey();
        assertThat(handlerResponse).isEqualTo(IGNORE);
    }


    @Test
    @Override
    public void deserializationErrorOnValue() {
        KafkaExceptionHandler.DeserializationHandlerResponse handlerResponse = setupDeserializationErrorOnValue();
        assertThat(handlerResponse).isEqualTo(IGNORE);
    }

    @Test
    @Override
    public void processingError() {
        KafkaExceptionHandler.DeserializationHandlerResponse handlerResponse = setupProcessingError();
        assertThat(handlerResponse).isEqualTo(IGNORE);
    }
}