package com.example.kafkacase.processing.persistence;

import com.example.kafkacase.event.Event;

import java.time.Instant;

public record ProcessedMessage(Event event, Instant receivedAt) {
}
