package com.example.demo.kafka.consumers;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractKafkaReader<K, V> implements KafkaReader {
    private final AtomicBoolean closed = new AtomicBoolean(false);

    protected final KafkaConsumer<DeserializerResult<K>, DeserializerResult<V>> consumer;

    public AbstractKafkaReader(KafkaConsumer<DeserializerResult<K>, DeserializerResult<V>> consumer) {
        this.consumer = consumer;
    }

    public abstract void pollLoop();

    protected boolean applicationRunning() {
        return !closed.get();
    }

    @Override
    public void run() {
        //required to support multithreaded processing (see the javadoc in KafkaProducer)
        try {
            pollLoop();

        } catch (
                WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) throw e;
        } finally {
            consumer.close();
        }
    }

    public void onShutdown() {
        //required to support multithreaded processing (see the javadoc in KafkaProducer)
        closed.set(true);
        consumer.wakeup();
    }

}
