package com.example.demo.kafka.consumers;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;
import java.util.Optional;

/**
 * Deserializer wrapping another deserializer for error management
 * Each message is returned as a {@link DeserializerResult}
 * This class can be used as is or can be overriden for convenient to avoid having to specify the inner deserializer
 *
 * @param <T> Expected data type returned by the deserializer
 */
public class ErrorWrapperDeserializer<T> implements Deserializer<DeserializerResult<T>> {
    public static final String KEY_WRAPPER_DESERIALIZER_CLASS = "deserializer.wrapper.key.class";
    public static final String VALUE_WRAPPER_DESERIALIZER_CLASS = "deserializer.wrapper.value.class";
    private Deserializer<T> innerDeserializer;

    //used by Kafka Client
    public ErrorWrapperDeserializer() {

    }

    public ErrorWrapperDeserializer(Deserializer<T> innerDeserializer) {
        this.innerDeserializer = innerDeserializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        configureDelegate(configs, isKey);
        innerDeserializer.configure(configs, isKey);
    }

    @SuppressWarnings("unchecked")
    private void configureDelegate(Map<String, ?> configs, boolean isKey) {
        if (innerDeserializer == null) {
            try {
                if (isKey) {
                    Object deserializerClass = Optional.ofNullable(configs.get(KEY_WRAPPER_DESERIALIZER_CLASS)).orElseThrow(() -> new IllegalStateException(KEY_WRAPPER_DESERIALIZER_CLASS + " is not configured"));
                    innerDeserializer = Utils.newInstance((String) deserializerClass, Deserializer.class);
                } else {
                    Object deserializerClass = Optional.ofNullable(configs.get(VALUE_WRAPPER_DESERIALIZER_CLASS)).orElseThrow(() -> new IllegalStateException(VALUE_WRAPPER_DESERIALIZER_CLASS + " is not configured"));
                    innerDeserializer = Utils.newInstance((String) deserializerClass, Deserializer.class);
                }
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Cannot configure deserializer", e);
            }
        }
    }

    @Override
    public DeserializerResult<T> deserialize(String topic, byte[] data) {
        if (data == null) {
            return new DeserializerResult<>();
        }
        try {
            return new DeserializerResult<>(innerDeserializer.deserialize(topic, data), data);
        } catch (Exception e) {
            return new DeserializerResult<>(data, e);
        }
    }

    @Override
    public void close() {
        if (innerDeserializer != null) {
            innerDeserializer.close();
        }
    }
}
