package com.example.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

@FunctionalInterface
public
interface OnValidRecordListener<K, V> {
    void onValidRecordEvent(ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record);
}
