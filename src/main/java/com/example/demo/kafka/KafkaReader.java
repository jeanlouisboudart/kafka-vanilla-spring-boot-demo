package com.example.demo.kafka;

import org.springframework.scheduling.annotation.Async;

public interface KafkaReader {

    @Async
    void run();

    void onShutdown();
}
