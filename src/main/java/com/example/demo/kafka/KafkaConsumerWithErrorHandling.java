package com.example.demo.kafka;

import lombok.experimental.Delegate;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Utils;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaConsumerWithErrorHandling<K, V> implements Closeable {
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;

    private final DeserializationExceptionHandler deserializationExceptionHandler;

    @Delegate(excludes = MinimalExcludeConsumer.class)
    private final Consumer<byte[], byte[]> consumer;

    public KafkaConsumerWithErrorHandling(Map<String, Object> originalConfig, DeserializationExceptionHandler deserializationExceptionHandler) {
        this.deserializationExceptionHandler = deserializationExceptionHandler;
        Map<String, Object> config = new HashMap<>(originalConfig);
        //rewrite deserializer
        String originalKeyDeserializer = (String) originalConfig.getOrDefault(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName());
        String originalValueDeserializer = (String) originalConfig.getOrDefault(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getCanonicalName());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        this.consumer = new KafkaConsumer<>(config);
        try {
            this.keyDeserializer = Utils.newInstance(originalKeyDeserializer, Deserializer.class);
            this.keyDeserializer.configure(originalConfig, true);
            this.valueDeserializer = Utils.newInstance(originalValueDeserializer, Deserializer.class);
            this.valueDeserializer.configure(originalConfig, false);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Cannot configure deserializer", e);
        }
    }

    public ConsumerRecords<K, V> poll(Duration timeout) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(timeout);
        Map<TopicPartition, List<ConsumerRecord<K, V>>> results = new HashMap<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            ConsumerRecord<K, V> deserializedRecord = deserialize(record);
            List<ConsumerRecord<K, V>> consumerRecords = results.computeIfAbsent(new TopicPartition(record.topic(), record.partition()), key -> new ArrayList<>());
            if (deserializedRecord != null) {
                consumerRecords.add(deserializedRecord);
            }

        }
        return new ConsumerRecords<>(results);

    }

    private ConsumerRecord<K, V> deserialize(ConsumerRecord<byte[], byte[]> record) {
        try {
            K key = keyDeserializer.deserialize(record.topic(), record.headers(), record.key());
            V value = valueDeserializer.deserialize(record.topic(), record.headers(), record.value());
            return new ConsumerRecord<>(record.topic(),
                    record.partition(),
                    record.offset(),
                    record.timestamp(),
                    record.timestampType(),
                    record.serializedKeySize(),
                    record.serializedValueSize(),
                    key,
                    value,
                    record.headers(),
                    record.leaderEpoch());
        } catch (SerializationException e) {
            DeserializationExceptionHandler.DeserializationHandlerResponse response = deserializationExceptionHandler.handle(record, e);
            switch (response) {
                case FAIL:
                    throw new SerializationException("Deserialization exception handler is set to fail upon  a deserialization error.", e);
                case CONTINUE:
                    return null;
                default:
                    throw new IllegalArgumentException("Response type not handled: " + response);
            }

        }
    }

    public interface MinimalExcludeConsumer<K, V> {
        ConsumerRecords<K, V> poll(Duration timeout);
    }

}
