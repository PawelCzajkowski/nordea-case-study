package com.example.kafkacase.processing;

import com.example.kafkacase.event.Event;

import java.time.Instant;

public record DeadLetterMessage(Event event, FailureMetadata failure, RawRecord rawRecord) {
    public DeadLetterMessage(Event event, FailureMetadata failure) {
        this(event, failure, null);
    }

    public record RawRecord(String key, byte[] value) {
    }

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
