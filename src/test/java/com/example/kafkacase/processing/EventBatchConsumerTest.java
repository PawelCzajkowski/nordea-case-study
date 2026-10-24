package com.example.kafkacase.processing;

import com.example.kafkacase.event.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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

    @ParameterizedTest
    @ValueSource(strings = {"{invalid JSON", "[1,2]", "{\"messageId\":\"invalid\"}"})
    void malformedJsonIsDeadLetteredLosslesslyAndTheNextEventIsProcessedBeforeAcknowledging(String rawJson)
            throws Exception {
        byte[] rawValue = rawJson.getBytes(StandardCharsets.UTF_8);
        ConsumerRecord<String, Event> malformed = malformedRecord(rawValue);
        Event successful = event("6d98bf37-e429-459b-8de0-2a96863076df");
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        when(service.process(null)).thenThrow(new NullPointerException("Event must not be null"));

        consumer.processBatch(List.of(malformed, record(successful, 8)), acknowledgment);

        var messageCaptor = org.mockito.ArgumentCaptor.forClass(DeadLetterMessage.class);
        var order = inOrder(deadLetterPublisher, service, acknowledgment);
        order.verify(deadLetterPublisher).publish(messageCaptor.capture());
        order.verify(service).process(successful);
        order.verify(acknowledgment).acknowledge();
        verify(service, never()).process(null);
        DeadLetterMessage message = messageCaptor.getValue();
        assertNull(message.event());
        assertEquals(1, message.failure().attempts());
        assertEquals("org.springframework.kafka.support.serializer.DeserializationException",
                message.failure().exceptionType());
        assertEquals("events", message.failure().sourceTopic());
        assertEquals(0, message.failure().sourcePartition());
        assertEquals(7L, message.failure().sourceOffset());
        var serialized = JSON.copy().findAndRegisterModules().valueToTree(message);
        assertEquals("malformed-key", serialized.at("/rawRecord/key").asText());
        assertArrayEquals(rawValue, serialized.at("/rawRecord/value").binaryValue());
    }

    @Test
    void unconfirmedMalformedRecordDeadLetterDoesNotBlockItsSiblingOrAcknowledgeTheBatch() {
        var malformed = malformedRecord("{invalid JSON".getBytes(StandardCharsets.UTF_8));
        Event successful = event("6d98bf37-e429-459b-8de0-2a96863076df");
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        when(service.process(null)).thenThrow(new NullPointerException("Event must not be null"));
        var publicationFailure = new DeadLetterPublicationException("Kafka DLQ unavailable", new IllegalStateException());
        org.mockito.Mockito.doThrow(publicationFailure).when(deadLetterPublisher)
                .publish(org.mockito.ArgumentMatchers.any());

        assertEquals(publicationFailure, assertThrows(DeadLetterPublicationException.class,
                () -> consumer.processBatch(List.of(malformed, record(successful, 8)), acknowledgment)));

        verify(service).process(successful);
        verify(service, never()).process(null);
        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void tombstoneIsDeadLetteredAndDoesNotPreventProcessingTheNextEvent() {
        var tombstone = new ConsumerRecord<String, Event>("events", 0, 7L, "deleted-key", null);
        Event successful = event("6d98bf37-e429-459b-8de0-2a96863076df");
        Acknowledgment acknowledgment = mock(Acknowledgment.class);

        consumer.processBatch(List.of(tombstone, record(successful, 8)), acknowledgment);

        var messageCaptor = org.mockito.ArgumentCaptor.forClass(DeadLetterMessage.class);
        verify(deadLetterPublisher).publish(messageCaptor.capture());
        assertEquals("deleted-key", messageCaptor.getValue().rawRecord().key());
        assertNull(messageCaptor.getValue().rawRecord().value());
        verify(service).process(successful);
        verify(service, never()).process(null);
        verify(acknowledgment).acknowledge();
    }

    private ConsumerRecord<String, Event> malformedRecord(byte[] rawValue) {
        var headers = new org.apache.kafka.common.header.internals.RecordHeaders();
        try (var deserializer = new ErrorHandlingDeserializer<>(new JsonDeserializer<>(Event.class, JSON))) {
            Event value = deserializer.deserialize("events", headers, rawValue);
            var record = new ConsumerRecord<>("events", 0, 7L, "malformed-key", value);
            headers.forEach(record.headers()::add);
            return record;
        }
    }

    private ConsumerRecord<String, Event> record(Event event, int offset) {
        return new ConsumerRecord<>("events", 0, offset, event.messageId().toString(), event);
    }

    private Event event(String messageId) {
        return new Event(UUID.fromString(messageId), "customer.updated", "1.0",
                Instant.parse("2026-10-23T12:00:00Z"), JSON.createObjectNode().put("customerId", messageId));
    }
}
