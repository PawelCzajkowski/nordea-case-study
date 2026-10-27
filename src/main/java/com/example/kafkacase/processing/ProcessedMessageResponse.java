package com.example.kafkacase.processing;

import com.example.kafkacase.processing.persistence.ProcessedMessage;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.UUID;

public record ProcessedMessageResponse(
        UUID messageId,
        String type,
        String schemaVersion,
        Instant occurredAt,
        JsonNode payload,
        Instant receivedAt
) {
    public static ProcessedMessageResponse from(ProcessedMessage processedMessage) {
        var event = processedMessage.event();
        return new ProcessedMessageResponse(event.messageId(), event.type(), event.schemaVersion(),
                event.occurredAt(), event.payload(), processedMessage.receivedAt());
    }
}
