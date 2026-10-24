package com.example.kafkacase.processing;

import com.example.kafkacase.event.Event;
import com.example.kafkacase.publishing.KafkaPublishingProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaDeadLetterPublisherTest {
    private final KafkaTemplate<String, DeadLetterMessage> kafkaTemplate = mock(KafkaTemplate.class);
    private final KafkaDeadLetterPublisher publisher = new KafkaDeadLetterPublisher(
            kafkaTemplate,
            new DeadLetterProperties("case-events.dead-letter"),
            new KafkaPublishingProperties("case-events", Duration.ofSeconds(2)));

    @Test
    void publishesTheDeadLetterToTheConfiguredTopicAndWaitsForKafkaConfirmation() {
        DeadLetterMessage message = message();
        CompletableFuture<SendResult<String, DeadLetterMessage>> confirmation = CompletableFuture.completedFuture(null);
        when(kafkaTemplate.send(eq("case-events.dead-letter"), eq(message.event().messageId().toString()), eq(message)))
                .thenReturn(confirmation);

        publisher.publish(message);

        verify(kafkaTemplate).send("case-events.dead-letter", message.event().messageId().toString(), message);
    }

    @Test
    void failsPublicationWhenKafkaDoesNotConfirmTheDeadLetter() {
        DeadLetterMessage message = message();
        CompletableFuture<SendResult<String, DeadLetterMessage>> confirmation = new CompletableFuture<>();
        confirmation.completeExceptionally(new IllegalStateException("broker rejected the record"));
        when(kafkaTemplate.send(eq("case-events.dead-letter"), eq(message.event().messageId().toString()), eq(message)))
                .thenReturn(confirmation);

        assertThrows(DeadLetterPublicationException.class, () -> publisher.publish(message));
    }

    private DeadLetterMessage message() {
        Event event = new Event(UUID.fromString("b395464a-605a-4cb7-928c-c9b5e0ddb9c0"),
                "customer.updated", "1.0", Instant.parse("2026-10-23T12:00:00Z"),
                new ObjectMapper().createObjectNode().put("customerId", "c-123"));
        return new DeadLetterMessage(event, new DeadLetterMessage.FailureMetadata(
                3, IllegalStateException.class.getName(), "database unavailable",
                Instant.parse("2026-10-23T12:30:00Z"), "case-events", 0, 7));
    }
}
