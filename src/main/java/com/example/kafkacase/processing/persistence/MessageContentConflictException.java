package com.example.kafkacase.processing.persistence;

import java.util.UUID;

public class MessageContentConflictException extends RuntimeException {
    public MessageContentConflictException(UUID messageId) {
        super("messageId is already associated with different Event content: " + messageId);
    }
}
