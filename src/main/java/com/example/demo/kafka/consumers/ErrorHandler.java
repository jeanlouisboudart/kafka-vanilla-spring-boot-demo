package com.example.demo.kafka.consumers;

public enum ErrorHandler {
    LogAndContinue,
    LogAndFail,
    DeadLetterQueue
}
