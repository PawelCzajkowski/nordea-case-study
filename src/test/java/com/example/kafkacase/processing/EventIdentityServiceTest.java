package com.example.kafkacase.processing;

import com.example.kafkacase.event.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EventIdentityServiceTest {
    private static final UUID MESSAGE_ID = UUID.fromString("4a4ce384-2bd6-49c4-8777-a0fbba178f39");
    private final Map<UUID, byte[]> identities = new HashMap<>();
    private final EventIdentityStore store = new EventIdentityStore() {
        @Override
        public boolean insertIfAbsent(UUID messageId, byte[] fingerprint) {
            if (identities.containsKey(messageId)) {
                return false;
            }
            identities.put(messageId, Arrays.copyOf(fingerprint, fingerprint.length));
            return true;
        }

        @Override
        public Optional<byte[]> findFingerprint(UUID messageId) {
            return Optional.ofNullable(identities.get(messageId));
        }
    };
    private final EventIdentityService service = new EventIdentityService(store, new ObjectMapper());

    @Test
    void identicalEventWithReorderedPayloadFieldsIsAcceptedAsTheSameIdentity() {
        Event first = event("{\"customerId\":\"c-123\",\"active\":true}");
        Event retry = event("{\"active\":true,\"customerId\":\"c-123\"}");

        service.register(first);
        assertDoesNotThrow(() -> service.register(retry));
    }

    @Test
    void differentContentCannotReuseAnExistingMessageId() {
        service.register(event("{\"customerId\":\"c-123\"}"));

        assertThrows(MessageContentConflictException.class,
                () -> service.register(event("{\"customerId\":\"c-999\"}")));
    }

    private Event event(String payloadJson) {
        try {
            return new Event(MESSAGE_ID, "customer.updated", "1.0",
                    Instant.parse("2026-10-23T12:00:00Z"), new ObjectMapper().readTree(payloadJson));
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }
}
