package com.example.kafkacase.processing;

import java.util.Optional;
import java.util.UUID;

public interface EventIdentityStore {
    boolean insertIfAbsent(UUID messageId, byte[] fingerprint);

    Optional<byte[]> findFingerprint(UUID messageId);
}
