package com.example.kafkacase.publishing;

import com.example.kafkacase.event.Event;
import com.example.kafkacase.processing.ProcessedMessageService;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class EventPublishingServiceTest {
    private static final UUID MESSAGE_ID = UUID.fromString("f8809d9e-0b82-4d65-8720-f70a23110fd3");

    @Test
    @SuppressWarnings("unchecked")
    void publishWaitsForKafkaAcknowledgementAndUsesConfiguredTopicAndMessageIdKey() throws Exception {
        KafkaTemplate<String, Event> kafkaTemplate = mock(KafkaTemplate.class);
        Event event = new Event(MESSAGE_ID, "customer.updated", "1.0", Instant.parse("2026-10-24T10:15:30Z"),
                JsonNodeFactory.instance.objectNode().put("customerId", "c-123"));
        CompletableFuture<SendResult<String, Event>> acknowledgement = new CompletableFuture<>();
        when(kafkaTemplate.send("configured-events", MESSAGE_ID.toString(), event)).thenReturn(acknowledgement);
        ProcessedMessageService processedMessageService = mock(ProcessedMessageService.class);
        when(processedMessageService.registerForPublication(event)).thenReturn(true);
        EventPublishingService service = new EventPublishingService(kafkaTemplate,
                new KafkaPublishingProperties("configured-events", Duration.ofSeconds(2)),
                processedMessageService);

        CompletableFuture<UUID> publication = CompletableFuture.supplyAsync(() -> service.publish(event));

        verify(kafkaTemplate, timeout(1_000)).send("configured-events", MESSAGE_ID.toString(), event);
        assertFalse(publication.isDone());
        acknowledgement.complete(null);

        assertEquals(MESSAGE_ID, publication.get(1, TimeUnit.SECONDS));
    }

    @Test
    @SuppressWarnings("unchecked")
    void brokerFailureAndAcknowledgementTimeoutAreReportedAsPublicationFailures() {
        KafkaTemplate<String, Event> kafkaTemplate = mock(KafkaTemplate.class);
        Event event = new Event(MESSAGE_ID, "customer.updated", "1.0", Instant.parse("2026-10-24T10:15:30Z"),
                JsonNodeFactory.instance.objectNode().put("customerId", "c-123"));
        ProcessedMessageService processedMessageService = mock(ProcessedMessageService.class);
        when(processedMessageService.registerForPublication(event)).thenReturn(true);
        EventPublishingService service = new EventPublishingService(kafkaTemplate,
                new KafkaPublishingProperties("configured-events", Duration.ofMillis(20)),
                processedMessageService);

        when(kafkaTemplate.send("configured-events", MESSAGE_ID.toString(), event))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));
        assertThrows(EventPublicationException.class, () -> service.publish(event));

        when(kafkaTemplate.send("configured-events", MESSAGE_ID.toString(), event))
                .thenReturn(new CompletableFuture<>());
        assertThrows(EventPublicationException.class, () -> service.publish(event));
    }
}
