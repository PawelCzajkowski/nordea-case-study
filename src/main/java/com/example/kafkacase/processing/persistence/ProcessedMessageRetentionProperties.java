package com.example.kafkacase.processing.persistence;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Objects;

@ConfigurationProperties(prefix = "app.processing")
record ProcessedMessageRetentionProperties(Duration retentionPeriod) {
    public ProcessedMessageRetentionProperties {
        Objects.requireNonNull(retentionPeriod, "retentionPeriod must be configured");
        if (retentionPeriod.isZero() || retentionPeriod.isNegative()) {
            throw new IllegalArgumentException("retentionPeriod must be greater than zero");
        }
    }
}
