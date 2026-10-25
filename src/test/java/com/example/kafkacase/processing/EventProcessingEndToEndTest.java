package com.example.kafkacase.processing;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.sql.Timestamp;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class EventProcessingEndToEndTest {
    private static final String TOPIC = "case-study-integration-events";
    private static final String DLQ_TOPIC = TOPIC + ".dlq";
    private static final String CONSUMER_GROUP = "message-service-integration-test";

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void configureInfrastructure(DynamicPropertyRegistry registry) {
        // Connect the application to this test's Kafka broker and consumer group.
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.kafka.consumer.group-id", () -> CONSUMER_GROUP);

        // Use the isolated PostgreSQL database for both persistence and Flyway migrations.
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);

        registry.add("app.kafka.topic", () -> TOPIC);
        registry.add("app.kafka.dead-letter.topic", () -> DLQ_TOPIC);

        // Run cleanup frequently so retention can be observed without a production-length wait.
        registry.add("app.processing.retention-period", () -> "30d");
        registry.add("app.processing.retention-cleanup-interval", () -> "100");
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private JwtDecoder jwtDecoder;

    @BeforeEach
    void clearStoredEvents() {
        // Each scenario starts with an empty read model and identity ledger.
        jdbcTemplate.execute("TRUNCATE processed_messages, event_identities");
    }

    @Test
    void clientCanPublishProcessAndRetrieveAnEventAndRetryItIdempotently() throws Exception {
        // Prepare an Event whose nanosecond timestamp will be rounded by PostgreSQL.
        UUID messageId = UUID.randomUUID();
        String originalEvent = eventJson(messageId, "c-123");

        // Publishing with the write scope succeeds after Kafka confirms acceptance.
        mockMvc.perform(post("/api/v1/events")
                        .with(SecurityMockMvcRequestPostProcessors.jwt()
                                .authorities(() -> "SCOPE_messages.write"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(originalEvent))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.messageId").value(messageId.toString()));

        // The same messageId cannot be reused for a different payload.
        mockMvc.perform(post("/api/v1/events")
                        .with(SecurityMockMvcRequestPostProcessors.jwt()
                                .authorities(() -> "SCOPE_messages.write"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(eventJson(messageId, "c-999")))
                .andExpect(status().isConflict());

        // Wait for asynchronous processing, then verify the persisted payload and timestamp precision.
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                mockMvc.perform(get("/api/v1/messages?page=0&size=100")
                                .with(SecurityMockMvcRequestPostProcessors.jwt()
                                        .authorities(() -> "SCOPE_messages.read")))
                        .andExpect(status().isOk())
                        .andExpect(jsonPath("$.content[?(@.messageId == '" + messageId + "')]").exists())
                        .andExpect(jsonPath("$.content[?(@.messageId == '" + messageId + "')].payload.customerId")
                                .value("c-123"))
                        .andExpect(jsonPath("$.content[?(@.messageId == '" + messageId + "')].occurredAt")
                                .value("2026-10-23T12:00:00.123457Z")));

        // An identical client retry remains valid after the Event has been persisted.
        mockMvc.perform(post("/api/v1/events")
                        .with(SecurityMockMvcRequestPostProcessors.jwt()
                                .authorities(() -> "SCOPE_messages.write"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(originalEvent))
                .andExpect(status().isAccepted());

        // Identity checks still distinguish changes smaller than PostgreSQL's timestamp precision.
        mockMvc.perform(post("/api/v1/events")
                        .with(SecurityMockMvcRequestPostProcessors.jwt()
                                .authorities(() -> "SCOPE_messages.write"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(originalEvent.replace(".123456789Z", ".123456788Z")))
                .andExpect(status().isConflict());

        // Publish a second Event to verify duplicate suppression and ordering across pages.
        UUID newerMessageId = UUID.randomUUID();

        mockMvc.perform(post("/api/v1/events")
                        .with(SecurityMockMvcRequestPostProcessors.jwt()
                                .authorities(() -> "SCOPE_messages.write"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(eventJson(newerMessageId, "c-456")))
                .andExpect(status().isAccepted());

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                mockMvc.perform(get("/api/v1/messages?page=0&size=100")
                                .with(SecurityMockMvcRequestPostProcessors.jwt()
                                        .authorities(() -> "SCOPE_messages.read")))
                        .andExpect(status().isOk())
                        .andExpect(jsonPath("$.totalElements").value(2)));

        // The repeated first Event must not increase the number of Processed messages.
        mockMvc.perform(get("/api/v1/messages?page=0&size=100")
                        .with(SecurityMockMvcRequestPostProcessors.jwt()
                                .authorities(() -> "SCOPE_messages.read")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalElements").value(2));

        // Page zero contains the newest Event; the older Event appears on the next page.
        mockMvc.perform(get("/api/v1/messages?page=0&size=1")
                        .with(SecurityMockMvcRequestPostProcessors.jwt()
                                .authorities(() -> "SCOPE_messages.read")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content[0].messageId").value(newerMessageId.toString()))
                .andExpect(jsonPath("$.totalPages").value(2));

        mockMvc.perform(get("/api/v1/messages?page=1&size=1")
                        .with(SecurityMockMvcRequestPostProcessors.jwt()
                                .authorities(() -> "SCOPE_messages.read")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content[0].messageId").value(messageId.toString()));

        // Age only the first Event and its identity so scheduled retention can remove both.
        jdbcTemplate.update(
                "UPDATE processed_messages SET received_at = ? WHERE message_id = ?",
                Timestamp.from(Instant.now().minus(Duration.ofDays(31))),
                messageId);

        jdbcTemplate.update(
                "UPDATE event_identities SET registered_at = ? WHERE message_id = ?",
                Timestamp.from(Instant.now().minus(Duration.ofDays(31))),
                messageId);

        // Cleanup removes the expired Event while preserving the newer one.
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                mockMvc.perform(get("/api/v1/messages?page=0&size=100")
                                .with(SecurityMockMvcRequestPostProcessors.jwt()
                                        .authorities(() -> "SCOPE_messages.read")))
                        .andExpect(status().isOk())
                        .andExpect(jsonPath("$.totalElements").value(1))
                        .andExpect(jsonPath("$.content[?(@.messageId == '" + messageId + "')]").doesNotExist())
                        .andExpect(jsonPath("$.content[?(@.messageId == '" + newerMessageId + "')]").exists()));

        // Once retention removes the identity, the UUID can identify new Event content.
        mockMvc.perform(post("/api/v1/events")
                        .with(SecurityMockMvcRequestPostProcessors.jwt()
                                .authorities(() -> "SCOPE_messages.write"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(eventJson(messageId, "c-after-retention")))
                .andExpect(status().isAccepted());

        // Wait for this final publication to finish before the next scenario resets the database.
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                mockMvc.perform(get("/api/v1/messages?page=0&size=100")
                                .with(SecurityMockMvcRequestPostProcessors.jwt()
                                        .authorities(() -> "SCOPE_messages.read")))
                        .andExpect(jsonPath("$.content[?(@.messageId == '" + messageId + "')].payload.customerId")
                                .value("c-after-retention")));
    }

    @Test
    void malformedKafkaValueIsPreservedInTheDeadLetterTopicWhileTheNextEventIsStored() throws Exception {
        // Include invalid UTF-8 to prove the DLQ preserves bytes rather than a lossy text conversion.
        String malformedKey = UUID.randomUUID().toString();
        byte[] malformedValue = new byte[]{'{', '"', (byte) 0xff};
        UUID messageId = UUID.randomUUID();

        // Read the DLQ independently without committing offsets for the application's consumer group.
        Map<String, Object> bootstrap = Map.of("bootstrap.servers", KAFKA.getBootstrapServers());
        Map<String, Object> dlqProperties = Map.of(
                "bootstrap.servers", KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "dlq-test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        try (var producer = new KafkaProducer<>(bootstrap, new StringSerializer(), new ByteArraySerializer());
             var dlqConsumer = new KafkaConsumer<>(dlqProperties, new StringDeserializer(), new StringDeserializer());
             var admin = Admin.create(bootstrap)) {
            dlqConsumer.subscribe(List.of(DLQ_TOPIC));

            // Keep both records in one partition so the malformed value precedes the valid Event.
            var malformed = producer.send(new ProducerRecord<>(TOPIC, 0, malformedKey, malformedValue))
                    .get(10, TimeUnit.SECONDS);

            var successful = producer.send(new ProducerRecord<>(TOPIC, 0, messageId.toString(),
                            eventJson(messageId, "after-malformed").getBytes(StandardCharsets.UTF_8)))
                    .get(10, TimeUnit.SECONDS);

            // A malformed value must not prevent the next Event from reaching the read API.
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    mockMvc.perform(get("/api/v1/messages?page=0&size=100")
                                    .with(SecurityMockMvcRequestPostProcessors.jwt()
                                            .authorities(() -> "SCOPE_messages.read")))
                            .andExpect(status().isOk())
                            .andExpect(jsonPath("$.content[?(@.messageId == '" + messageId + "')].payload.customerId")
                                    .value("after-malformed")));

            // Locate this scenario's Dead-letter message and inspect its original bytes and source metadata.
            await().atMost(Duration.ofSeconds(30)).until(() -> {
                for (var record : dlqConsumer.poll(Duration.ofMillis(100))) {
                    if (malformedKey.equals(record.key())) {
                        var message = objectMapper.readTree(record.value());

                        assertTrue(message.path("event").isNull());
                        assertEquals(malformedKey, message.at("/rawRecord/key").asText());
                        assertArrayEquals(malformedValue, message.at("/rawRecord/value").binaryValue());

                        assertEquals(1, message.at("/failure/attempts").asInt());
                        assertEquals("org.springframework.kafka.support.serializer.DeserializationException",
                                message.at("/failure/exceptionType").asText());
                        assertEquals(TOPIC, message.at("/failure/sourceTopic").asText());
                        assertEquals(malformed.partition(), message.at("/failure/sourcePartition").asInt());
                        assertEquals(malformed.offset(), message.at("/failure/sourceOffset").asLong());

                        return true;
                    }
                }

                return false;
            });

            // Committing past the valid Event confirms that the consumer advanced beyond both outcomes.
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
                var offsets = admin.listConsumerGroupOffsets(CONSUMER_GROUP)
                        .partitionsToOffsetAndMetadata().get(5, TimeUnit.SECONDS);
                var committed = offsets.get(new TopicPartition(TOPIC, successful.partition()));

                assertTrue(committed != null && committed.offset() > successful.offset());
            });
        }
    }

    private String eventJson(UUID messageId, String customerId) {
        return """
                {
                  "messageId": "%s",
                  "type": "customer.updated",
                  "schemaVersion": "1.0",
                  "occurredAt": "2026-10-23T12:00:00.123456789Z",
                  "payload": {"customerId": "%s"}
                }
                """.formatted(messageId, customerId);
    }
}
