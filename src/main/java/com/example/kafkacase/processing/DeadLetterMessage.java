package com.example.kafkacase.processing;

import com.example.kafkacase.event.Event;

import java.time.Instant;

public record DeadLetterMessage(Event event, FailureMetadata failure) {
    public record FailureMetadata(
            int attempts,
            String exceptionType,
            String reason,
            Instant failedAt,
            String sourceTopic,
            int sourcePartition,
            long sourceOffset
    ) {
    }
}
