package com.example.kafkacase.processing;

import com.example.kafkacase.event.Event;
import com.example.kafkacase.processing.persistence.ProcessedMessageService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class EventBatchConsumerTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Instant FAILED_AT = Instant.parse("2026-10-23T12:30:00Z");
    private static final Clock CLOCK = Clock.fixed(FAILED_AT, ZoneOffset.UTC);

    private final ProcessedMessageService service = mock(ProcessedMessageService.class);
    private final DeadLetterPublisher deadLetterPublisher = mock(DeadLetterPublisher.class);
    private final ProcessingRetryProperties retryProperties = new ProcessingRetryProperties(3, Duration.ZERO);
    private final EventBatchConsumer consumer = new EventBatchConsumer(service, deadLetterPublisher, retryProperties, CLOCK);

    @Test
    void retriesOnlyTheFailedEventAndAcknowledgesAfterEveryEventIsPersisted() {
        Event first = event();
        Event second = event();
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        when(service.process(first)).thenThrow(new IllegalStateException("temporary database issue"))
                .thenReturn(null);

        consumer.processBatch(List.of(record(first, 0), record(second, 1)), acknowledgment);

        var order = inOrder(service, acknowledgment);
        order.verify(service, times(2)).process(first);
        order.verify(service).process(second);
        order.verify(acknowledgment).acknowledge();
        verifyNoInteractions(deadLetterPublisher);
    }

    @Test
    void exhaustedEventPublishesAMessageWithFailureMetadataBeforeAcknowledging() {
        Event failed = event();
        Event successful = event();
        var source = record(failed, 7);
        var processingFailure = new IllegalStateException("database unavailable");
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        when(service.process(failed)).thenThrow(processingFailure);

        consumer.processBatch(List.of(source, record(successful, 8)), acknowledgment);

        var order = inOrder(service, deadLetterPublisher, acknowledgment);
        var messages = org.mockito.ArgumentCaptor.forClass(DeadLetterMessage.class);
        order.verify(service, times(3)).process(failed);
        order.verify(deadLetterPublisher).publish(messages.capture());
        order.verify(service).process(successful);
        order.verify(acknowledgment).acknowledge();

        DeadLetterMessage message = messages.getValue();
        assertEquals(failed, message.event());
        assertNull(message.rawRecord());
        assertEquals(new DeadLetterMessage.FailureMetadata(3, IllegalStateException.class.getName(),
                "database unavailable", FAILED_AT, "events", 0, 7L), message.failure());
    }

    @Test
    void continuesProcessingSiblingsButDoesNotAcknowledgeWhenDeadLetterPublicationFails() {
        Event failed = event();
        Event successful = event();
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        when(service.process(failed)).thenThrow(new IllegalStateException("database unavailable"));
        var publicationFailure = new IllegalStateException("Kafka DLQ unavailable");
        doThrow(publicationFailure).when(deadLetterPublisher).publish(any(DeadLetterMessage.class));

        assertSame(publicationFailure, assertThrows(IllegalStateException.class,
                () -> consumer.processBatch(List.of(record(failed, 7), record(successful, 8)), acknowledgment)));

        verify(service).process(successful);
        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void deadLetterPreparationFailureAlsoPreservesSiblingProgressAndLeavesTheBatchUnconfirmed() {
        Event failed = event();
        Event successful = event();
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        when(service.process(failed)).thenThrow(new IllegalStateException("database unavailable"));

        // Exercise a real preparation failure, before publish(message) is called.
        Clock clock = mock(Clock.class);
        var preparationFailure = new IllegalStateException("clock unavailable");
        when(clock.instant()).thenThrow(preparationFailure);
        var listener = new EventBatchConsumer(service, deadLetterPublisher, retryProperties, clock);

        assertSame(preparationFailure, assertThrows(IllegalStateException.class,
                () -> listener.processBatch(List.of(record(failed, 7), record(successful, 8)), acknowledgment)));

        verify(service).process(successful);
        verifyNoInteractions(deadLetterPublisher);
        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void multipleDeadLetterFailuresAreRetainedAfterAllSiblingsHaveBeenProcessed() {
        Event first = event();
        Event second = event();
        Event successful = event();
        var firstSource = record(first, 7);
        var secondSource = record(second, 8);
        var processingFailure = new IllegalStateException("database unavailable");
        var firstFailure = new IllegalStateException("first DLQ failure");
        var secondFailure = new IllegalStateException("second DLQ failure");
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        when(service.process(first)).thenThrow(processingFailure);
        when(service.process(second)).thenThrow(processingFailure);
        doThrow(firstFailure, secondFailure).when(deadLetterPublisher).publish(any(DeadLetterMessage.class));

        assertSame(firstFailure, assertThrows(IllegalStateException.class,
                () -> consumer.processBatch(
                        List.of(firstSource, secondSource, record(successful, 9)), acknowledgment)));

        assertArrayEquals(new Throwable[]{secondFailure}, firstFailure.getSuppressed());
        verify(service).process(successful);
        verify(acknowledgment, never()).acknowledge();
    }

    @ParameterizedTest
    @ValueSource(strings = {"{invalid JSON", "[1,2]", "{\"messageId\":\"invalid\"}"})
    void malformedValuesPublishTheirOriginalBytesWithoutProcessingRetries(String rawJson) {
        byte[] rawValue = rawJson.getBytes(StandardCharsets.UTF_8);
        var malformed = malformedRecord(rawValue);
        Event successful = event();
        Acknowledgment acknowledgment = mock(Acknowledgment.class);

        consumer.processBatch(List.of(malformed, record(successful, 8)), acknowledgment);

        var messages = org.mockito.ArgumentCaptor.forClass(DeadLetterMessage.class);
        var order = inOrder(deadLetterPublisher, service, acknowledgment);
        order.verify(deadLetterPublisher).publish(messages.capture());
        order.verify(service).process(successful);
        order.verify(acknowledgment).acknowledge();
        verify(service, never()).process(null);
        DeadLetterMessage message = messages.getValue();
        assertNull(message.event());
        assertEquals("malformed-key", message.rawRecord().key());
        assertArrayEquals(rawValue, message.rawRecord().value());
        assertEquals(1, message.failure().attempts());
        assertEquals(DeserializationException.class.getName(), message.failure().exceptionType());
        assertEquals(FAILED_AT, message.failure().failedAt());
        assertEquals("events", message.failure().sourceTopic());
        assertEquals(0, message.failure().sourcePartition());
        assertEquals(7L, message.failure().sourceOffset());
    }

    @Test
    void unconfirmedMalformedRecordDeadLetterDoesNotBlockItsSiblingOrAcknowledgeTheBatch() {
        var malformed = malformedRecord("{invalid JSON".getBytes(StandardCharsets.UTF_8));
        Event successful = event();
        Acknowledgment acknowledgment = mock(Acknowledgment.class);
        var publicationFailure = new DeadLetterPublicationException("Kafka DLQ unavailable", new IllegalStateException());
        doThrow(publicationFailure).when(deadLetterPublisher)
                .publish(any(DeadLetterMessage.class));

        assertSame(publicationFailure, assertThrows(DeadLetterPublicationException.class,
                () -> consumer.processBatch(List.of(malformed, record(successful, 8)), acknowledgment)));

        verify(service).process(successful);
        verify(service, never()).process(null);
        verify(acknowledgment, never()).acknowledge();
    }

    @Test
    void tombstonePublishesItsOriginalKeyAndNullValueWithoutBlockingTheNextEvent() {
        var tombstone = new ConsumerRecord<String, Event>("events", 0, 7L, "deleted-key", null);
        Event successful = event();
        Acknowledgment acknowledgment = mock(Acknowledgment.class);

        consumer.processBatch(List.of(tombstone, record(successful, 8)), acknowledgment);

        var messages = org.mockito.ArgumentCaptor.forClass(DeadLetterMessage.class);
        var order = inOrder(deadLetterPublisher, service, acknowledgment);
        order.verify(deadLetterPublisher).publish(messages.capture());
        order.verify(service).process(successful);
        order.verify(acknowledgment).acknowledge();
        DeadLetterMessage message = messages.getValue();
        assertNull(message.event());
        assertEquals("deleted-key", message.rawRecord().key());
        assertNull(message.rawRecord().value());
        assertEquals(1, message.failure().attempts());
        assertEquals(IllegalArgumentException.class.getName(), message.failure().exceptionType());
        assertEquals("Kafka record contains no Event value", message.failure().reason());
        verify(service, never()).process(null);
    }

    @ParameterizedTest
    @CsvSource(value = {"NULL,IllegalStateException", "'',''"}, nullValues = "NULL")
    void missingFailureReasonFallsBackToTheClassNameButAnEmptyReasonStaysEmpty(String reason, String expected) {
        Event failed = event();
        when(service.process(failed)).thenThrow(new IllegalStateException(reason));

        consumer.processBatch(List.of(record(failed, 7)), mock(Acknowledgment.class));

        var messages = org.mockito.ArgumentCaptor.forClass(DeadLetterMessage.class);
        verify(deadLetterPublisher).publish(messages.capture());
        assertEquals(expected, messages.getValue().failure().reason());
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

    private Event event() {
        UUID messageId = UUID.randomUUID();
        return new Event(messageId, "customer.updated", "1.0",
                java.time.Instant.parse("2026-10-23T12:00:00Z"),
                JSON.createObjectNode().put("customerId", messageId.toString()));
    }
}
