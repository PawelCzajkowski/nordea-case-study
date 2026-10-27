package com.example.kafkacase.processing.persistence;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Repository
class JdbcEventIdentityStore implements EventIdentityStore {
    private final JdbcTemplate jdbcTemplate;

    public JdbcEventIdentityStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public boolean insertIfAbsent(UUID messageId, byte[] fingerprint) {
        return jdbcTemplate.update("""
                        INSERT INTO event_identities (message_id, fingerprint)
                        VALUES (?, ?)
                        ON CONFLICT (message_id) DO NOTHING
                        """,
                messageId,
                fingerprint) == 1;
    }

    @Override
    public Optional<byte[]> findFingerprint(UUID messageId) {
        return jdbcTemplate.query("SELECT fingerprint FROM event_identities WHERE message_id = ?",
                result -> result.next() ? Optional.of(result.getBytes("fingerprint")) : Optional.empty(), messageId);
    }

    @Override
    public int deleteRegisteredBefore(Instant cutoff) {
        return jdbcTemplate.update(
                "DELETE FROM event_identities WHERE registered_at < ?",
                Timestamp.from(cutoff));
    }
}
