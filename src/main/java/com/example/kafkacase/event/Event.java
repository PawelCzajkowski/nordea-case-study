package com.example.kafkacase.event;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.Instant;
import java.util.UUID;

public record Event(
        @NotNull UUID messageId,
        @NotBlank String type,
        @NotBlank String schemaVersion,
        @NotNull Instant occurredAt,
        @NotNull JsonNode payload
) {
}
