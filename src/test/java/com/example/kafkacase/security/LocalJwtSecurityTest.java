package com.example.kafkacase.security;

import com.example.kafkacase.event.Event;
import com.example.kafkacase.processing.ProcessedMessageController;
import com.example.kafkacase.processing.ProcessedMessagePage;
import com.example.kafkacase.processing.ProcessedMessageService;
import com.example.kafkacase.publishing.EventPublishingController;
import com.example.kafkacase.publishing.EventPublishingService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest({EventPublishingController.class, ProcessedMessageController.class})
@Import({SecurityConfiguration.class, LocalJwtConfiguration.class})
@ActiveProfiles("local")
class LocalJwtSecurityTest {
    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private EventPublishingService eventPublishingService;

    @MockitoBean
    private ProcessedMessageService processedMessageService;

    @Test
    void localBearerTokenCanReadMessagesWithoutAnIdentityProvider() throws Exception {
        when(processedMessageService.findPage(0, 50)).thenReturn(new ProcessedMessagePage(List.of(), 0));

        mockMvc.perform(get("/api/v1/messages")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer local-dev-token"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalElements").value(0));

        verify(processedMessageService).findPage(0, 50);
    }

    @Test
    void localBearerTokenCanPublishEventsWithoutAnIdentityProvider() throws Exception {
        UUID messageId = UUID.fromString("f8809d9e-0b82-4d65-8720-f70a23110fd3");
        when(eventPublishingService.publish(any(Event.class))).thenReturn(messageId);

        mockMvc.perform(post("/api/v1/events")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer local-dev-token")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "messageId": "f8809d9e-0b82-4d65-8720-f70a23110fd3",
                                  "type": "customer.updated",
                                  "schemaVersion": "1.0",
                                  "occurredAt": "2026-10-24T10:15:30Z",
                                  "payload": {"customerId": "c-123"}
                                }
                                """))
                .andExpect(status().isAccepted())
                .andExpect(jsonPath("$.messageId").value(messageId.toString()));
    }

    @Test
    void localProfileStillRequiresABearerToken() throws Exception {
        mockMvc.perform(get("/api/v1/messages"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void localProfileRejectsOtherBearerTokens() throws Exception {
        mockMvc.perform(get("/api/v1/messages")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer some-other-token"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void mockDecoderIsNotRegisteredByDefault() {
        new ApplicationContextRunner()
                .withUserConfiguration(LocalJwtConfiguration.class)
                .run(context -> assertThat(context).doesNotHaveBean(JwtDecoder.class));
    }

    @Test
    void mockDecoderIsNotRegisteredForTheProductionProfile() {
        new ApplicationContextRunner()
                .withPropertyValues("spring.profiles.active=production")
                .withUserConfiguration(LocalJwtConfiguration.class)
                .run(context -> assertThat(context).doesNotHaveBean(JwtDecoder.class));
    }
}
