package com.example.kafkacase.processing;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Objects;

@ConfigurationProperties(prefix = "app.processing")
public record ProcessedMessageRetentionProperties(Duration retentionPeriod) {
    public ProcessedMessageRetentionProperties {
        Objects.requireNonNull(retentionPeriod, "retentionPeriod must be configured");
        if (retentionPeriod.isZero() || retentionPeriod.isNegative()) {
            throw new IllegalArgumentException("retentionPeriod must be greater than zero");
        }
    }
}
