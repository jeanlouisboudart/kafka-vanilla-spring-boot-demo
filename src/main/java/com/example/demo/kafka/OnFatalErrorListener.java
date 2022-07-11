package com.example.demo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

@FunctionalInterface
public
interface OnFatalErrorListener<K, V> {
    void onFatalErrorEvent(ErrorType errorType, ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record, Exception exception);
}
