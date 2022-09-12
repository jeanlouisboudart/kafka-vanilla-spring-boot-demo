package com.example.demo.kafka.consumers;

import com.example.demo.kafka.ErrorType;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class PropagateFatalErrorListener<K, V> implements OnFatalErrorListener<K, V> {

    @Override
    public void onFatalErrorEvent(ErrorType errorType, ConsumerRecord<DeserializerResult<K>, DeserializerResult<V>> record, Exception exception) {
        throw new RuntimeException(exception);
    }
}
