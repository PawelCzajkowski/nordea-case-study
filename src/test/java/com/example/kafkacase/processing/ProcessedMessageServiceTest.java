package com.example.kafkacase.processing;

import com.example.kafkacase.event.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.Clock;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ProcessedMessageServiceTest {
    private static final UUID MESSAGE_ID = UUID.fromString("4a4ce384-2bd6-49c4-8777-a0fbba178f39");
    private static final Instant RECEIVED_AT = Instant.parse("2026-10-24T10:15:30Z");

    private final ProcessedMessageStore store = mock(ProcessedMessageStore.class);
    private final EventIdentityService eventIdentityService = mock(EventIdentityService.class);
    private final ProcessedMessageService service = new ProcessedMessageService(
            store, Clock.fixed(RECEIVED_AT, ZoneOffset.UTC), eventIdentityService);

    @Test
    void processingAnEventMakesItAvailableAsAProcessedMessage() {
        Event event = event("customer-42");
        ProcessedMessage processed = new ProcessedMessage(event, RECEIVED_AT);
        when(store.insertIfAbsent(eq(event), eq(RECEIVED_AT))).thenReturn(true);
        when(store.findByMessageId(MESSAGE_ID)).thenReturn(Optional.of(processed));

        assertEquals(processed, service.process(event));
    }

    @Test
    void replayingIdenticalEventReturnsTheExistingProcessedMessage() {
        Event event = event("customer-42");
        ProcessedMessage existing = new ProcessedMessage(event, Instant.parse("2026-10-20T08:00:00Z"));
        when(store.insertIfAbsent(eq(event), eq(RECEIVED_AT))).thenReturn(false);
        when(store.findByMessageId(MESSAGE_ID)).thenReturn(Optional.of(existing));

        assertEquals(existing, service.process(event));
    }

    @Test
    void reusingMessageIdForDifferentEventContentIsAConflict() {
        Event existingEvent = event("customer-42");
        when(store.insertIfAbsent(eq(event("customer-99")), eq(RECEIVED_AT))).thenReturn(false);
        when(store.findByMessageId(MESSAGE_ID)).thenReturn(
                Optional.of(new ProcessedMessage(existingEvent, RECEIVED_AT)));

        assertThrows(MessageContentConflictException.class, () -> service.process(event("customer-99")));
    }

    private Event event(String customerId) {
        return new Event(MESSAGE_ID, "customer.updated", "1.0", Instant.parse("2026-10-23T12:00:00Z"),
                new ObjectMapper().createObjectNode().put("customerId", customerId));
    }
}
