package com.example.kafkacase.publishing;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties("app.kafka")
public record KafkaPublishingProperties(String topic, Duration ackTimeout) {
}
