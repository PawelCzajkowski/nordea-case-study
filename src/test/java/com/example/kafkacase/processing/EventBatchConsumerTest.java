package com.example.kafkacase.processing;

import com.example.kafkacase.event.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class EventBatchConsumerTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    private final ProcessedMessageService service = mock(ProcessedMessageService.class);
    private final EventBatchConsumer consumer = new EventBatchConsumer(service);

    @Test
    void batchIsAcknowledgedOnlyAfterEveryEventIsPersisted() {
        Event first = event("b395464a-605a-4cb7-928c-c9b5e0ddb9c0");
        Event second = event("6d98bf37-e429-459b-8de0-2a96863076df");
        Acknowledgment acknowledgment = mock(Acknowledgment.class);

        consumer.processBatch(List.of(record(first, 0), record(second, 1)), acknowledgment);

        var order = inOrder(service, acknowledgment);
        order.verify(service).process(first);
        order.verify(service).process(second);
        order.verify(acknowledgment).acknowledge();
    }

    @Test
    void batchRemainsUnacknowledgedWhenPersistenceFails() {
        Event first = event("b395464a-605a-4cb7-928c-c9b5e0ddb9c0");
        Event second = event("6d98bf37-e429-459b-8de0-2a96863076df");
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        when(service.process(second)).thenThrow(new IllegalStateException("database unavailable"));

        assertThrows(IllegalStateException.class,
                () -> consumer.processBatch(List.of(record(first, 0), record(second, 1)), acknowledgment));

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
