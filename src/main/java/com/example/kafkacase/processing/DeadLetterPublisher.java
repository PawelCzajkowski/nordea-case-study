package com.example.kafkacase.processing;

public interface DeadLetterPublisher {
    /** Returns only after Kafka has confirmed publication of the dead-letter message. */
    void publish(DeadLetterMessage message);
}
