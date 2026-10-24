package com.example.kafkacase.processing;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("app.kafka.dead-letter")
public record DeadLetterProperties(String topic) {
    public DeadLetterProperties {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("dead-letter topic must not be blank");
        }
    }
}
