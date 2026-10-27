package com.example.kafkacase.processing.persistence;

import com.example.kafkacase.event.Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@JdbcTest(properties = "app.processing.retention-period=30d")
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(ProcessedMessageServiceIntegrationTest.ModuleConfiguration.class)
@Transactional(propagation = Propagation.NOT_SUPPORTED)
@Testcontainers
class ProcessedMessageServiceIntegrationTest {
    private static final Instant NOW = Instant.parse("2026-10-27T12:00:00Z");
    private static final Instant CUTOFF = NOW.minus(Duration.ofDays(30));

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void configureDatabase(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired
    private ProcessedMessageService service;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void clearStoredEvents() {
        jdbcTemplate.execute("TRUNCATE processed_messages, event_identities");
    }

    @Test
    void identicalRetriesKeepTheOriginalProcessedMessageAndItsReceiptTime() {
        Event event = event(UUID.randomUUID(), "customer-42");

        // A pending reservation still needs publication, including an identical client retry.
        assertTrue(service.registerForPublication(event));
        assertTrue(service.registerForPublication(event));
        service.process(event);

        Instant originalReceipt = NOW.minusSeconds(60);
        jdbcTemplate.update("UPDATE processed_messages SET received_at = ? WHERE message_id = ?",
                Timestamp.from(originalReceipt), event.messageId());

        ProcessedMessage replay = service.process(event);

        assertEquals(originalReceipt, replay.receivedAt());
        assertFalse(service.registerForPublication(event));
        assertEquals(List.of(replay), service.findPage(0, 10).content());
        assertEquals(1, service.findPage(0, 10).totalElements());
    }

    @ParameterizedTest
    @CsvSource({
            "2026-10-23T12:00:00.123456789Z, 2026-10-23T12:00:00.123457Z",
            "2026-10-23T12:00:00.123456123Z, 2026-10-23T12:00:00.123456Z",
            "2026-10-23T12:00:00.999999789Z, 2026-10-23T12:00:01Z"
    })
    void postgresRoundingAcceptsReplayButDoesNotHideNanosecondConflicts(Instant originalTime, Instant storedTime) {
        Event original = event(UUID.randomUUID(), "customer-42", originalTime);

        assertEquals(storedTime, service.process(original).event().occurredAt());
        assertEquals(storedTime, service.process(original).event().occurredAt());
        assertFalse(service.registerForPublication(original));

        Event changed = event(original.messageId(), "customer-42", originalTime.plusNanos(1));
        assertThrows(MessageContentConflictException.class, () -> service.registerForPublication(changed));
        assertThrows(MessageContentConflictException.class, () -> service.process(changed));
        assertEquals(storedTime, service.findPage(0, 10).content().getFirst().event().occurredAt());
    }

    @Test
    void objectFieldOrderIsIgnoredButChangedContentAndArrayOrderConflict() throws Exception {
        UUID messageId = UUID.randomUUID();
        Instant time = Instant.parse("2026-10-23T12:00:00Z");
        Event original = new Event(messageId, "customer.updated", "1", time,
                objectMapper.readTree("{\"customer\":{\"id\":42,\"active\":true},\"values\":[1,2]}"));
        Event reordered = new Event(messageId, original.type(), original.schemaVersion(), time,
                objectMapper.readTree("{\"values\":[1,2],\"customer\":{\"active\":true,\"id\":42}}"));
        Event changed = new Event(messageId, original.type(), original.schemaVersion(), time,
                objectMapper.readTree("{\"customer\":{\"id\":42,\"active\":true},\"values\":[2,1]}"));

        service.registerForPublication(original);
        assertTrue(service.registerForPublication(reordered));
        service.process(reordered);
        assertFalse(service.registerForPublication(original));
        assertThrows(MessageContentConflictException.class, () -> service.process(changed));
        assertThrows(MessageContentConflictException.class,
                () -> service.registerForPublication(event(messageId, "different-content")));
        assertEquals(original.payload(), service.findPage(0, 10).content().getFirst().event().payload());
    }

    @Test
    void failedPersistenceRollsBackTheIdentityReservation() {
        Event valid = event(UUID.randomUUID(), "customer-42");
        Event invalid = new Event(valid.messageId(), null, valid.schemaVersion(), valid.occurredAt(), valid.payload());

        // The real NOT NULL constraint fails after identity registration in the same transaction.
        assertThrows(DataIntegrityViolationException.class, () -> service.process(invalid));
        assertEquals(0, service.findPage(0, 10).totalElements());
        assertTrue(service.registerForPublication(valid));
        assertEquals(valid, service.process(valid).event());
    }

    @Test
    void retentionExpiresOldDataAndPendingIdentitiesButKeepsTheCutoffInclusive() {
        Event expired = event(UUID.randomUUID(), "expired");
        Event atCutoff = event(UUID.randomUUID(), "at-cutoff");
        Event recent = event(UUID.randomUUID(), "recent");
        Event pending = event(UUID.randomUUID(), "pending");
        service.process(expired);
        service.process(atCutoff);
        service.process(recent);
        service.registerForPublication(pending);
        age(expired.messageId(), CUTOFF.minusSeconds(1), CUTOFF.minusSeconds(1));
        age(atCutoff.messageId(), CUTOFF, CUTOFF);
        age(pending.messageId(), CUTOFF.minusSeconds(1), NOW);

        service.deleteExpiredMessages();

        assertEquals(2, service.findPage(0, 10).totalElements());
        assertFalse(service.registerForPublication(atCutoff));
        assertFalse(service.registerForPublication(recent));
        assertTrue(service.registerForPublication(event(expired.messageId(), "new-after-expiry")));
        assertTrue(service.registerForPublication(event(pending.messageId(), "new-after-expiry")));
    }

    @Test
    void identityAndProcessedMessageKeepTheirSeparateRetentionStartTimes() {
        Event retainedMessage = event(UUID.randomUUID(), "retained-message");
        Event retainedIdentity = event(UUID.randomUUID(), "retained-identity");
        service.process(retainedMessage);
        service.process(retainedIdentity);
        age(retainedMessage.messageId(), CUTOFF.minusSeconds(1), NOW);
        age(retainedIdentity.messageId(), NOW, CUTOFF.minusSeconds(1));

        service.deleteExpiredMessages();

        assertEquals(List.of(retainedMessage), service.findPage(0, 10).content().stream()
                .map(ProcessedMessage::event).toList());

        // A surviving Processed message still guards conflicts after its identity expires.
        Event changed = event(retainedMessage.messageId(), "conflicting-content");
        assertThrows(MessageContentConflictException.class, () -> service.registerForPublication(changed));
        assertThrows(MessageContentConflictException.class, () -> service.process(changed));
        // Both failed operations must roll back their newly inserted identity.
        assertFalse(service.registerForPublication(retainedMessage));

        // A surviving identity permits replay, but still rejects changed content.
        assertTrue(service.registerForPublication(retainedIdentity));
        assertThrows(MessageContentConflictException.class,
                () -> service.registerForPublication(event(retainedIdentity.messageId(), "conflicting-content")));
        service.process(retainedIdentity);
        assertEquals(2, service.findPage(0, 10).totalElements());
    }

    @Test
    void failedIdentityCleanupRollsBackProcessedMessageCleanup() {
        Event event = event(UUID.randomUUID(), "expired");
        ProcessedMessage original = service.process(event);
        age(event.messageId(), CUTOFF.minusSeconds(1), CUTOFF.minusSeconds(1));

        // A test-only foreign key makes the second DELETE fail after the first DELETE succeeds.
        jdbcTemplate.execute("CREATE TABLE retention_blocker (message_id UUID REFERENCES event_identities(message_id))");
        try {
            jdbcTemplate.update("INSERT INTO retention_blocker VALUES (?)", event.messageId());

            assertThrows(DataIntegrityViolationException.class, service::deleteExpiredMessages);
            assertEquals(List.of(new ProcessedMessage(original.event(), CUTOFF.minusSeconds(1))),
                    service.findPage(0, 10).content());
            assertFalse(service.registerForPublication(event));
        } finally {
            jdbcTemplate.execute("DROP TABLE retention_blocker");
        }
    }

    @Test
    void pagesHaveStableOrderingAndReportTheFullTotal() {
        Event first = event(UUID.fromString("00000000-0000-0000-0000-000000000001"), "first");
        Event second = event(UUID.fromString("00000000-0000-0000-0000-000000000002"), "second");
        service.process(first);
        service.process(second);

        assertEquals(List.of(second), service.findPage(0, 1).content().stream().map(ProcessedMessage::event).toList());
        assertEquals(List.of(first), service.findPage(1, 1).content().stream().map(ProcessedMessage::event).toList());
        assertEquals(2, service.findPage(1, 1).totalElements());
        assertTrue(service.findPage(2, 1).content().isEmpty());
        assertEquals(2, service.findPage(2, 1).totalElements());
    }

    private void age(UUID messageId, Instant registeredAt, Instant receivedAt) {
        jdbcTemplate.update("UPDATE event_identities SET registered_at = ? WHERE message_id = ?",
                Timestamp.from(registeredAt), messageId);
        jdbcTemplate.update("UPDATE processed_messages SET received_at = ? WHERE message_id = ?",
                Timestamp.from(receivedAt), messageId);
    }

    private Event event(UUID messageId, String customerId) {
        return event(messageId, customerId, Instant.parse("2026-10-23T12:00:00Z"));
    }

    private Event event(UUID messageId, String customerId, Instant occurredAt) {
        return new Event(messageId, "customer.updated", "1", occurredAt,
                objectMapper.createObjectNode().put("customerId", customerId));
    }

    @TestConfiguration(proxyBeanMethods = false)
    @ComponentScan(basePackageClasses = ProcessedMessageService.class)
    static class ModuleConfiguration {
        @Bean
        Clock moduleClock() {
            return Clock.fixed(NOW, ZoneOffset.UTC);
        }

        @Bean
        ObjectMapper moduleObjectMapper() {
            return new ObjectMapper().findAndRegisterModules();
        }
    }
}
