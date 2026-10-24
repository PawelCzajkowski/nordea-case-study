package com.example.kafkacase.processing;

import com.example.kafkacase.event.Event;
import com.example.kafkacase.security.SecurityConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(ProcessedMessageController.class)
@Import(SecurityConfiguration.class)
class ProcessedMessageApiContractTest {
    private static final UUID MESSAGE_ID = UUID.fromString("09b8747e-7d25-46e0-b868-7d0d620fb034");
    private static final ObjectMapper JSON = new ObjectMapper();

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private ProcessedMessageService processedMessageService;

    @MockitoBean
    private JwtDecoder jwtDecoder;

    @Test
    void clientWithReadScopeCanRetrievePagedProcessedMessages() throws Exception {
        Event event = new Event(MESSAGE_ID, "customer.updated", "1.0",
                Instant.parse("2026-10-23T12:00:00Z"), JSON.createObjectNode().put("customerId", "c-123"));
        when(processedMessageService.findPage(1, 2)).thenReturn(
                new ProcessedMessagePage(List.of(new ProcessedMessage(event, Instant.parse("2026-10-24T12:00:00Z"))), 5));

        mockMvc.perform(get("/api/v1/messages?page=1&size=2")
                        .with(SecurityMockMvcRequestPostProcessors.jwt()
                                .authorities(() -> "SCOPE_messages.read")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.page").value(1))
                .andExpect(jsonPath("$.size").value(2))
                .andExpect(jsonPath("$.totalElements").value(5))
                .andExpect(jsonPath("$.totalPages").value(3))
                .andExpect(jsonPath("$.content[0].messageId").value(MESSAGE_ID.toString()))
                .andExpect(jsonPath("$.content[0].payload.customerId").value("c-123"));

        verify(processedMessageService).findPage(1, 2);
    }

    @Test
    void readApiRequiresAuthentication() throws Exception {
        mockMvc.perform(get("/api/v1/messages")).andExpect(status().isUnauthorized());
    }

    @Test
    void readApiRequiresTheReadScope() throws Exception {
        mockMvc.perform(get("/api/v1/messages")
                        .with(SecurityMockMvcRequestPostProcessors.jwt()
                                .authorities(() -> "SCOPE_messages.write")))
                .andExpect(status().isForbidden());
    }

    @Test
    void invalidPageSizeIsRejected() throws Exception {
        mockMvc.perform(get("/api/v1/messages?size=101")
                        .with(SecurityMockMvcRequestPostProcessors.jwt()
                                .authorities(() -> "SCOPE_messages.read"))
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(status().isBadRequest());
    }
}
