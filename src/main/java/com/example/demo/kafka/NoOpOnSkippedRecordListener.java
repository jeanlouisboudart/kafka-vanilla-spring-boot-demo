package com.example.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class NoOpOnSkippedRecordListener<K, V> implements OnSkippedRecordListener<K, V> {
    @Override
    public void onSkippedRecordEvent(ErrorType errorType, ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record, Exception exception) {

    }
}
