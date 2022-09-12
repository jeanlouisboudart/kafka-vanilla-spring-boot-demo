package com.example.demo.kafka.consumers;

/**
 * Either returns the deserialized message or flag the message as invalid
 */
public class DeserializerResult<T> {
    private final Exception exception;
    private final T deserializedValue;
    private final byte[] valueAsBytes;

    public DeserializerResult() {
        this(null, null, null);
    }

    public DeserializerResult(byte[] valueAsBytes, Exception exception) {
        this(null, valueAsBytes, exception);
    }

    public DeserializerResult(T value, byte[] valueAsBytes) {
        this(value, valueAsBytes, null);
    }


    public DeserializerResult(T value, byte[] valueAsBytes, Exception exception) {
        this.deserializedValue = value;
        this.valueAsBytes = valueAsBytes;
        this.exception = exception;
    }

    public Exception getException() {
        return exception;
    }

    public T getDeserializedValue() {
        return deserializedValue;
    }

    public byte[] getValueAsBytes() {
        return valueAsBytes;
    }

    public Boolean valid() {
        return exception == null;
    }

}
