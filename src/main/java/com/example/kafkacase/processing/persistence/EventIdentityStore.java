package com.example.kafkacase.processing.persistence;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

interface EventIdentityStore {
    boolean insertIfAbsent(UUID messageId, byte[] fingerprint);

    Optional<byte[]> findFingerprint(UUID messageId);

    int deleteRegisteredBefore(Instant cutoff);
}
