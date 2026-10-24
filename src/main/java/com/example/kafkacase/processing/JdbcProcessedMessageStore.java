package com.example.kafkacase.processing;

import com.example.kafkacase.event.Event;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public class JdbcProcessedMessageStore implements ProcessedMessageStore {
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private final RowMapper<ProcessedMessage> rowMapper;

    public JdbcProcessedMessageStore(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
        this.rowMapper = this::mapRow;
    }

    @Override
    public boolean insertIfAbsent(Event event, Instant receivedAt) {
        String sql = """
                INSERT INTO processed_messages (message_id, event_type, schema_version, occurred_at, payload, received_at)
                VALUES (?, ?, ?, ?, CAST(? AS jsonb), ?)
                ON CONFLICT (message_id) DO NOTHING
                """;
        return jdbcTemplate.update(sql,
                event.messageId(),
                event.type(),
                event.schemaVersion(),
                Timestamp.from(event.occurredAt()),
                event.payload().toString(),
                Timestamp.from(receivedAt)) == 1;
    }

    @Override
    public Optional<ProcessedMessage> findByMessageId(UUID messageId) {
        List<ProcessedMessage> messages = jdbcTemplate.query(
                "SELECT * FROM processed_messages WHERE message_id = ?",
                rowMapper,
                messageId);
        return messages.stream().findFirst();
    }

    @Override
    public ProcessedMessagePage findPage(int page, int size) {
        long offset = Math.multiplyExact((long) page, size);
        List<ProcessedMessage> content = jdbcTemplate.query("""
                        SELECT * FROM processed_messages
                        ORDER BY received_at DESC, message_id DESC
                        LIMIT ? OFFSET ?
                        """,
                rowMapper,
                size,
                offset);
        Long totalElements = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM processed_messages", Long.class);
        return new ProcessedMessagePage(content, totalElements == null ? 0 : totalElements);
    }

    private ProcessedMessage mapRow(ResultSet result, int rowNumber) throws SQLException {
        try {
            Event event = new Event(
                    result.getObject("message_id", UUID.class),
                    result.getString("event_type"),
                    result.getString("schema_version"),
                    result.getTimestamp("occurred_at").toInstant(),
                    objectMapper.readTree(result.getString("payload"))
            );
            return new ProcessedMessage(event, result.getTimestamp("received_at").toInstant());
        } catch (JsonProcessingException exception) {
            throw new SQLException("Stored Event payload is not valid JSON", exception);
        }
    }
}
