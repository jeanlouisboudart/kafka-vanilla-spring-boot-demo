package com.example.demo.kafka.consumers;

import com.example.demo.kafka.ErrorType;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@FunctionalInterface
public
interface OnSkippedRecordListener<K, V> {
    void onSkippedRecordEvent(ErrorType errorType, ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record, Exception exception);
}
