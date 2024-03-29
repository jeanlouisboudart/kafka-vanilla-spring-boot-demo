package com.example.demo.kafka.consumers;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ErrorWrapperDeserializerTest {

    @Test
    public void configureKey() {
        try (ErrorWrapperDeserializer<String> deserializer = new ErrorWrapperDeserializer<>()) {
            final Map<String, Object> config = new HashMap<>();
            config.put(ErrorWrapperDeserializer.KEY_WRAPPER_DESERIALIZER_CLASS, StringDeserializer.class.getCanonicalName());
            deserializer.configure(config, true);
        }
    }

    @Test
    public void configureValue() {
        try (ErrorWrapperDeserializer<String> deserializer = new ErrorWrapperDeserializer<>()) {
            final Map<String, Object> config = new HashMap<>();
            config.put(ErrorWrapperDeserializer.VALUE_WRAPPER_DESERIALIZER_CLASS, StringDeserializer.class.getCanonicalName());
            deserializer.configure(config, false);
        }
    }

    @Test
    public void configureShouldFailWhenWrapperKeyIsNotConfigured() {
        try (ErrorWrapperDeserializer<String> deserializer = new ErrorWrapperDeserializer<>()) {
            final Map<String, Object> config = new HashMap<>();
            IllegalStateException illegalStateException = assertThrows(IllegalStateException.class, () -> deserializer.configure(config, true));
            assertThat(illegalStateException.getMessage()).contains(ErrorWrapperDeserializer.KEY_WRAPPER_DESERIALIZER_CLASS);
        }
    }

    @Test
    public void configureShouldFailWhenWrapperValueIsNotConfigured() {
        try (ErrorWrapperDeserializer<String> deserializer = new ErrorWrapperDeserializer<>()) {
            final Map<String, Object> config = new HashMap<>();
            IllegalStateException illegalStateException = assertThrows(IllegalStateException.class, () -> deserializer.configure(config, false));
            assertThat(illegalStateException.getMessage()).contains(ErrorWrapperDeserializer.VALUE_WRAPPER_DESERIALIZER_CLASS);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void deserializeValidString() {
        final Deserializer<String> delegateDeserializer = mock(Deserializer.class);
        final String topic = "mytopic";
        final String input = "valid string";
        final byte[] inputAsBytes = input.getBytes();
        try (ErrorWrapperDeserializer<String> deserializer = new ErrorWrapperDeserializer<>(delegateDeserializer)) {

            Mockito.when(delegateDeserializer.deserialize(topic, inputAsBytes)).thenReturn(input);
            DeserializerResult<String> result = deserializer.deserialize(topic, inputAsBytes);

            assertThat(result).isNotNull();
            assertThat(result.getDeserializedValue()).isEqualTo(input);
            assertThat(result.getValueAsBytes()).isEqualTo(inputAsBytes);
            assertThat(result.getException()).isNull();
            assertThat(result.valid()).isTrue();
        }
    }


    @Test
    @SuppressWarnings("unchecked")
    public void deserializationError() {
        final Deserializer<String> delegateDeserializer = mock(Deserializer.class);
        final String topic = "mytopic";
        final byte[] inputAsBytes = String.valueOf(42).getBytes();

        try (ErrorWrapperDeserializer<String> deserializer = new ErrorWrapperDeserializer<>(delegateDeserializer)) {
            Mockito.when(delegateDeserializer.deserialize(topic, inputAsBytes)).thenThrow(new SerializationException("BOOM"));
            DeserializerResult<String> result = deserializer.deserialize(topic, inputAsBytes);
            assertThat(result).isNotNull();
            assertThat(result.getDeserializedValue()).isNull();
            assertThat(result.getValueAsBytes()).isEqualTo(inputAsBytes);
            assertThat(result.getException()).isInstanceOf(SerializationException.class);
            assertThat(result.valid()).isFalse();
        }
    }


    @Test
    @SuppressWarnings("unchecked")
    public void close() {
        final Deserializer<String> delegateDeserializer = mock(Deserializer.class);
        ErrorWrapperDeserializer<String> deserializer = new ErrorWrapperDeserializer<>(delegateDeserializer);
        deserializer.close();
        verify(delegateDeserializer).close();
    }
}