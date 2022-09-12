package com.example.demo.kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;

@FunctionalInterface
public
interface OnValidRecordListener<K, V> {
    void onValidRecordEvent(ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record);
}
