package com.example.kafkacase.processing;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties("app.kafka.processing.retry")
public record ProcessingRetryProperties(int maxAttempts, Duration backoff) {
    public ProcessingRetryProperties {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least one");
        }
        if (backoff == null || backoff.isNegative()) {
            throw new IllegalArgumentException("backoff must not be null or negative");
        }
    }
}
