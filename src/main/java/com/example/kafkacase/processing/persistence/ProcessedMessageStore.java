package com.example.kafkacase.processing.persistence;

import com.example.kafkacase.event.Event;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

interface ProcessedMessageStore {
    boolean insertIfAbsent(Event event, Instant receivedAt);

    Optional<ProcessedMessage> findByMessageId(UUID messageId);

    ProcessedMessagePage findPage(int page, int size);

    int deleteReceivedBefore(Instant cutoff);
}
