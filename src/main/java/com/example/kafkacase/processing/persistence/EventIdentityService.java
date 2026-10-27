package com.example.kafkacase.processing.persistence;

import com.example.kafkacase.event.Event;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Service;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Map;
import java.util.TreeMap;

@Service
class EventIdentityService {
    private final EventIdentityStore store;
    private final ObjectMapper objectMapper;

    public EventIdentityService(EventIdentityStore store, ObjectMapper objectMapper) {
        this.store = store;
        this.objectMapper = objectMapper;
    }

    public void register(Event event) {
        byte[] fingerprint = fingerprint(event);

        // The first publication reserves this messageId for the original Event content.
        if (store.insertIfAbsent(event.messageId(), fingerprint)) {
            return;
        }

        byte[] originalFingerprint = store.findFingerprint(event.messageId())
                .orElseThrow(() -> new IllegalStateException("Registered Event identity could not be read"));

        // Identical retries are allowed; a different fingerprint represents a content conflict.
        if (!MessageDigest.isEqual(originalFingerprint, fingerprint)) {
            throw new MessageContentConflictException(event.messageId());
        }
    }

    void deleteRegisteredBefore(Instant cutoff) {
        store.deleteRegisteredBefore(cutoff);
    }

    private byte[] fingerprint(Event event) {
        // Preserve the original timestamp precision when calculating the Event's identity.
        ObjectNode identity = objectMapper.createObjectNode();
        identity.put("type", event.type());
        identity.put("schemaVersion", event.schemaVersion());
        identity.put("occurredAt", event.occurredAt().toString());
        identity.set("payload", canonicalize(event.payload()));

        try {
            return MessageDigest.getInstance("SHA-256").digest(objectMapper.writeValueAsBytes(identity));
        } catch (JsonProcessingException | NoSuchAlgorithmException exception) {
            throw new IllegalStateException("Event content could not be fingerprinted", exception);
        }
    }

    private JsonNode canonicalize(JsonNode value) {
        if (value.isObject()) {
            // Sort object fields recursively so JSON property order does not affect identity.
            ObjectNode canonical = objectMapper.createObjectNode();
            Map<String, JsonNode> fields = new TreeMap<>();

            value.fields().forEachRemaining(field -> fields.put(field.getKey(), field.getValue()));
            fields.forEach((key, child) -> canonical.set(key, canonicalize(child)));

            return canonical;
        }

        if (value.isArray()) {
            // Array element order remains part of the Event content.
            ArrayNode canonical = objectMapper.createArrayNode();
            value.forEach(child -> canonical.add(canonicalize(child)));

            return canonical;
        }

        return value.deepCopy();
    }
}
