package com.example.kafkacase.processing;

public class DeadLetterPublicationException extends RuntimeException {
    public DeadLetterPublicationException(String message, Throwable cause) {
        super(message, cause);
    }
}
