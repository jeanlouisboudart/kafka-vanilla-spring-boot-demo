package com.example.demo.kafka.consumers;

import com.example.demo.kafka.ErrorType;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@FunctionalInterface
public
interface OnFatalErrorListener<K, V> {
    void onFatalErrorEvent(ErrorType errorType, ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record, Exception exception);
}
