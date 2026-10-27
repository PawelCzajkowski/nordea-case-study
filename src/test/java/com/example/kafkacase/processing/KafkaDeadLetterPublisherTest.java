package com.example.kafkacase.processing;

import com.example.kafkacase.event.Event;
import com.example.kafkacase.publishing.KafkaPublishingProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaDeadLetterPublisherTest {
    private static final String TOPIC = "case-events.dead-letter";
    private static final UUID MESSAGE_ID = UUID.fromString("b395464a-605a-4cb7-928c-c9b5e0ddb9c0");
    private static final Instant FAILED_AT = Instant.parse("2026-10-23T12:30:00Z");
    private static final ObjectMapper JSON = new ObjectMapper();

    private final KafkaTemplate<String, DeadLetterMessage> kafkaTemplate = mock(KafkaTemplate.class);
    private final KafkaDeadLetterPublisher publisher = publisher(Duration.ofSeconds(2));

    @Test
    void publishesThePreparedMessageWithItsEventUuidKeyAndPreservesTheWireFormat() throws Exception {
        DeadLetterMessage message = message();

        publishAndVerify(message, MESSAGE_ID.toString());

        assertEquals(JSON.readTree("""
                {
                  "event": {
                    "messageId": "b395464a-605a-4cb7-928c-c9b5e0ddb9c0",
                    "type": "customer.updated",
                    "schemaVersion": "1.0",
                    "occurredAt": 1792756800.0,
                    "payload": {"customerId": "c-123"}
                  },
                  "failure": {
                    "attempts": 3,
                    "exceptionType": "java.lang.IllegalStateException",
                    "reason": "database unavailable",
                    "failedAt": 1792758600.0,
                    "sourceTopic": "events",
                    "sourcePartition": 2,
                    "sourceOffset": 7
                  },
                  "rawRecord": null
                }
                """), wireFormat(message));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = "malformed-key")
    void publishesThePreparedMalformedMessageWithItsOriginalKeyAndBytes(String key) throws Exception {
        byte[] rawValue = new byte[]{'{', '"', (byte) 0xff};
        var message = new DeadLetterMessage(null, new DeadLetterMessage.FailureMetadata(
                1, DeserializationException.class.getName(), "invalid Event", FAILED_AT, "events", 2, 7),
                new DeadLetterMessage.RawRecord(key, rawValue));

        publishAndVerify(message, key);
        JsonNode serialized = wireFormat(message);

        assertTrue(serialized.path("event").isNull());
        assertEquals(JSON.valueToTree(key), serialized.at("/rawRecord/key"));
        assertArrayEquals(rawValue, serialized.at("/rawRecord/value").binaryValue());
    }

    @Test
    void publishesThePreparedTombstoneMessageWithANullRawValueAndTheOriginalKey() throws Exception {
        var message = new DeadLetterMessage(null, new DeadLetterMessage.FailureMetadata(
                1, IllegalArgumentException.class.getName(), "Kafka record contains no Event value",
                FAILED_AT, "events", 2, 7), new DeadLetterMessage.RawRecord("deleted-key", null));

        publishAndVerify(message, "deleted-key");
        JsonNode serialized = wireFormat(message);

        assertTrue(serialized.path("event").isNull());
        assertEquals("deleted-key", serialized.at("/rawRecord/key").asText());
        assertTrue(serialized.at("/rawRecord/value").isNull());
    }

    @Test
    void publicationDoesNotReturnUntilKafkaConfirmsTheSend() throws Exception {
        DeadLetterMessage message = message();
        CompletableFuture<SendResult<String, DeadLetterMessage>> confirmation = new CompletableFuture<>();
        when(kafkaTemplate.send(TOPIC, MESSAGE_ID.toString(), message)).thenReturn(confirmation);

        try (var executor = Executors.newSingleThreadExecutor()) {
            try {
                var publication = executor.submit(() -> publisher.publish(message));

                verify(kafkaTemplate, timeout(1_000)).send(TOPIC, MESSAGE_ID.toString(), message);
                assertFalse(publication.isDone());

                confirmation.complete(null);
                publication.get(1, TimeUnit.SECONDS);
            } finally {
                confirmation.complete(null);
            }
        }
    }

    @Test
    void rejectedConfirmationReportsThePublicationFailureWithItsCause() {
        var brokerFailure = new IllegalStateException("broker rejected the record");
        when(kafkaTemplate.send(eq(TOPIC), eq(MESSAGE_ID.toString()), any(DeadLetterMessage.class)))
                .thenReturn(CompletableFuture.failedFuture(brokerFailure));

        var failure = assertThrows(DeadLetterPublicationException.class, () -> publisher.publish(message()));

        assertEquals("Kafka did not confirm dead-letter publication", failure.getMessage());
        assertSame(brokerFailure, failure.getCause().getCause());
    }

    @Test
    void timeoutLeavesPublicationUnconfirmed() {
        when(kafkaTemplate.send(eq(TOPIC), eq(MESSAGE_ID.toString()), any(DeadLetterMessage.class)))
                .thenReturn(new CompletableFuture<>());

        var failure = assertThrows(DeadLetterPublicationException.class,
                () -> publisher(Duration.ofMillis(1)).publish(message()));

        assertInstanceOf(TimeoutException.class, failure.getCause());
    }

    @Test
    void interruptedConfirmationRestoresTheThreadFlagAndReportsFailure() {
        when(kafkaTemplate.send(eq(TOPIC), eq(MESSAGE_ID.toString()), any(DeadLetterMessage.class)))
                .thenReturn(new CompletableFuture<>());

        try {
            Thread.currentThread().interrupt();

            var failure = assertThrows(DeadLetterPublicationException.class, () -> publisher.publish(message()));

            assertTrue(Thread.currentThread().isInterrupted());
            assertInstanceOf(InterruptedException.class, failure.getCause());
            assertEquals("Interrupted while publishing a dead-letter message", failure.getMessage());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void synchronousSendFailurePropagatesUnchanged() {
        var sendFailure = new SerializationException("could not serialize the dead-letter message");
        when(kafkaTemplate.send(eq(TOPIC), eq(MESSAGE_ID.toString()), any(DeadLetterMessage.class)))
                .thenThrow(sendFailure);

        assertSame(sendFailure, assertThrows(SerializationException.class, () -> publisher.publish(message())));
    }

    private KafkaDeadLetterPublisher publisher(Duration ackTimeout) {
        return new KafkaDeadLetterPublisher(kafkaTemplate, new DeadLetterProperties(TOPIC),
                new KafkaPublishingProperties("case-events", ackTimeout));
    }

    private void publishAndVerify(DeadLetterMessage message, String expectedKey) {
        when(kafkaTemplate.send(TOPIC, expectedKey, message)).thenReturn(CompletableFuture.completedFuture(null));

        publisher.publish(message);

        verify(kafkaTemplate).send(TOPIC, expectedKey, message);
    }

    private JsonNode wireFormat(DeadLetterMessage message) throws Exception {
        try (var serializer = new JsonSerializer<DeadLetterMessage>()) {
            serializer.setAddTypeInfo(false);
            return JSON.readTree(serializer.serialize(TOPIC, message));
        }
    }

    private DeadLetterMessage message() {
        Event event = new Event(MESSAGE_ID, "customer.updated", "1.0", Instant.parse("2026-10-23T12:00:00Z"),
                JSON.createObjectNode().put("customerId", "c-123"));
        return new DeadLetterMessage(event, new DeadLetterMessage.FailureMetadata(
                3, IllegalStateException.class.getName(), "database unavailable", FAILED_AT, "events", 2, 7));
    }
}
