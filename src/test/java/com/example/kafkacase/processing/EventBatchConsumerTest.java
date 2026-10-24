package com.example.kafkacase.processing;

import com.example.kafkacase.event.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class EventBatchConsumerTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    private final ProcessedMessageService service = mock(ProcessedMessageService.class);
    private final DeadLetterPublisher deadLetterPublisher = mock(DeadLetterPublisher.class);
    private final ProcessingRetryProperties retryProperties = new ProcessingRetryProperties(3, Duration.ZERO);
    private final Clock clock = Clock.fixed(Instant.parse("2026-10-23T12:30:00Z"), ZoneOffset.UTC);
    private final EventBatchConsumer consumer = new EventBatchConsumer(service, deadLetterPublisher, retryProperties, clock);

    @Test
    void retriesOnlyTheFailedEventAndAcknowledgesAfterEveryEventIsPersisted() {
        Event first = event("b395464a-605a-4cb7-928c-c9b5e0ddb9c0");
        Event second = event("6d98bf37-e429-459b-8de0-2a96863076df");
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        when(service.process(first)).thenThrow(new IllegalStateException("temporary database issue"))
                .thenReturn(null);

        consumer.processBatch(List.of(record(first, 0), record(second, 1)), acknowledgment);

        var order = inOrder(service, acknowledgment);
        order.verify(service, org.mockito.Mockito.times(2)).process(first);
        order.verify(service).process(second);
        order.verify(acknowledgment).acknowledge();
    }

    @Test
    void exhaustedEventIsDeadLetteredWithFailureMetadataBeforeBatchAcknowledgement() {
        Event failed = event("b395464a-605a-4cb7-928c-c9b5e0ddb9c0");
        Event successful = event("6d98bf37-e429-459b-8de0-2a96863076df");
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        when(service.process(failed)).thenThrow(new IllegalStateException("database unavailable"));

        consumer.processBatch(List.of(record(failed, 7), record(successful, 8)), acknowledgment);

        var messageCaptor = org.mockito.ArgumentCaptor.forClass(DeadLetterMessage.class);
        var order = inOrder(service, deadLetterPublisher, acknowledgment);
        order.verify(service, org.mockito.Mockito.times(3)).process(failed);
        order.verify(deadLetterPublisher).publish(messageCaptor.capture());
        order.verify(service).process(successful);
        order.verify(acknowledgment).acknowledge();

        DeadLetterMessage message = messageCaptor.getValue();
        assertEquals(failed, message.event());
        assertEquals(3, message.failure().attempts());
        assertEquals(IllegalStateException.class.getName(), message.failure().exceptionType());
        assertEquals("database unavailable", message.failure().reason());
        assertEquals("events", message.failure().sourceTopic());
        assertEquals(0, message.failure().sourcePartition());
        assertEquals(7L, message.failure().sourceOffset());
        assertEquals(Instant.parse("2026-10-23T12:30:00Z"), message.failure().failedAt());
    }

    @Test
    void continuesProcessingSiblingsButDoesNotAcknowledgeWhenDeadLetterPublicationFails() {
        Event failed = event("b395464a-605a-4cb7-928c-c9b5e0ddb9c0");
        Event successful = event("6d98bf37-e429-459b-8de0-2a96863076df");
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        when(service.process(failed)).thenThrow(new IllegalStateException("database unavailable"));
        org.mockito.Mockito.doThrow(new IllegalStateException("Kafka DLQ unavailable"))
                .when(deadLetterPublisher).publish(org.mockito.ArgumentMatchers.any());

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> consumer.processBatch(List.of(record(failed, 7), record(successful, 8)), acknowledgment));

        assertEquals("Kafka DLQ unavailable", failure.getMessage());
        verify(service).process(successful);
        verify(acknowledgment, never()).acknowledge();
    }

    private ConsumerRecord<String, Event> record(Event event, int offset) {
        return new ConsumerRecord<>("events", 0, offset, event.messageId().toString(), event);
    }

    private Event event(String messageId) {
        return new Event(UUID.fromString(messageId), "customer.updated", "1.0",
                Instant.parse("2026-10-23T12:00:00Z"), JSON.createObjectNode().put("customerId", messageId));
    }
}
