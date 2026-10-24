package com.example.kafkacase.publishing;

import com.example.kafkacase.event.Event;
import com.example.kafkacase.security.SecurityConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(EventPublishingController.class)
@Import(SecurityConfiguration.class)
class EventPublishingApiContractTest {
    private static final UUID MESSAGE_ID = UUID.fromString("f8809d9e-0b82-4d65-8720-f70a23110fd3");

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private EventPublishingService eventPublishingService;

    @MockitoBean
    private JwtDecoder jwtDecoder;

    @Test
    void clientCanPublishAValidEventWithTheWriteScope() throws Exception {
        when(eventPublishingService.publish(any(Event.class))).thenReturn(MESSAGE_ID);

        mockMvc.perform(post("/api/v1/events")
                        .with(SecurityMockMvcRequestPostProcessors.jwt().authorities(() -> "SCOPE_messages.write"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "messageId": "f8809d9e-0b82-4d65-8720-f70a23110fd3",
                                  "type": "customer.updated",
                                  "schemaVersion": "1.0",
                                  "occurredAt": "2026-10-24T10:15:30Z",
                                  "payload": {"customerId": "c-123", "active": true}
                                }
                                """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.messageId").value(MESSAGE_ID.toString()));
    }

    @Test
    void publishingRequiresAuthentication() throws Exception {
        mockMvc.perform(post("/api/v1/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validEventJson()))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void publishingRequiresTheWriteScope() throws Exception {
        mockMvc.perform(post("/api/v1/events")
                        .with(SecurityMockMvcRequestPostProcessors.jwt()
                                .authorities(() -> "SCOPE_messages.read"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validEventJson()))
                .andExpect(status().isForbidden());
    }

    @Test
    void invalidEventIsRejectedBeforePublishing() throws Exception {
        mockMvc.perform(post("/api/v1/events")
                        .with(SecurityMockMvcRequestPostProcessors.jwt()
                                .authorities(() -> "SCOPE_messages.write"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validEventJson().replace("customer.updated", " ")))
                .andExpect(status().isBadRequest());
    }

    @Test
    void brokerFailureIsReportedAsServiceUnavailable() throws Exception {
        when(eventPublishingService.publish(any(Event.class)))
                .thenThrow(new EventPublicationException("broker unavailable", new IllegalStateException()));

        mockMvc.perform(post("/api/v1/events")
                        .with(SecurityMockMvcRequestPostProcessors.jwt()
                                .authorities(() -> "SCOPE_messages.write"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(validEventJson()))
                .andExpect(status().isServiceUnavailable());
    }

    private String validEventJson() {
        return """
                {
                  "messageId": "f8809d9e-0b82-4d65-8720-f70a23110fd3",
                  "type": "customer.updated",
                  "schemaVersion": "1.0",
                  "occurredAt": "2026-10-24T10:15:30Z",
                  "payload": {"customerId": "c-123", "active": true}
                }
                """;
    }
}
