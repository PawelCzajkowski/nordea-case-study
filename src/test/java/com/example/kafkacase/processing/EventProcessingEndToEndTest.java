package com.example.kafkacase.processing;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.containers.PostgreSQLContainer;

import java.time.Duration;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@Testcontainers
class EventProcessingEndToEndTest {
    private static final String TOPIC = "case-study-integration-events";

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void configureInfrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.kafka.consumer.group-id", () -> "message-service-integration-test");
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("app.kafka.topic", () -> TOPIC);
    }

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private JwtDecoder jwtDecoder;

    @Test
    void clientCanPublishProcessAndRetrieveAnEventAndRetryItIdempotently() throws Exception {
        UUID messageId = UUID.randomUUID();
        String originalEvent = eventJson(messageId, "c-123");

        mockMvc.perform(post("/api/v1/events")
                        .with(SecurityMockMvcRequestPostProcessors.jwt()
                                .authorities(() -> "SCOPE_messages.write"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(originalEvent))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.messageId").value(messageId.toString()));

        mockMvc.perform(post("/api/v1/events")
                        .with(SecurityMockMvcRequestPostProcessors.jwt()
                                .authorities(() -> "SCOPE_messages.write"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(eventJson(messageId, "c-999")))
                .andExpect(status().isConflict());

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                mockMvc.perform(get("/api/v1/messages?page=0&size=100")
                                .with(SecurityMockMvcRequestPostProcessors.jwt()
                                        .authorities(() -> "SCOPE_messages.read")))
                        .andExpect(status().isOk())
                        .andExpect(jsonPath("$.content[?(@.messageId == '" + messageId + "')]").exists())
                        .andExpect(jsonPath("$.content[?(@.messageId == '" + messageId + "')].payload.customerId")
                                .value("c-123")));

        mockMvc.perform(post("/api/v1/events")
                        .with(SecurityMockMvcRequestPostProcessors.jwt()
                                .authorities(() -> "SCOPE_messages.write"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(originalEvent))
                .andExpect(status().isAccepted());

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

        mockMvc.perform(get("/api/v1/messages?page=0&size=100")
                        .with(SecurityMockMvcRequestPostProcessors.jwt()
                                .authorities(() -> "SCOPE_messages.read")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalElements").value(2));

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
    }

    private String eventJson(UUID messageId, String customerId) {
        return """
                {
                  "messageId": "%s",
                  "type": "customer.updated",
                  "schemaVersion": "1.0",
                  "occurredAt": "2026-10-23T12:00:00Z",
                  "payload": {"customerId": "%s"}
                }
                """.formatted(messageId, customerId);
    }
}
