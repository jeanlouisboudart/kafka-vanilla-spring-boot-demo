package com.example.demo.services;

import org.springframework.scheduling.annotation.Async;

public interface KafkaReader {

    @Async
    void run();

    void onShutdown();
}
