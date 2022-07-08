package com.example.demo.config;

public enum ErrorHandler {
    LogAndContinue,
    LogAndFail,
    DeadLetterQueue
}
