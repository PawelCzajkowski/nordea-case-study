package com.example.kafkacase.health;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.TestExecutionListeners.MergeMode;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.web.ServletTestExecutionListener;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest
@AutoConfigureMockMvc
@TestExecutionListeners(
        listeners = {ServletTestExecutionListener.class, DependencyInjectionTestExecutionListener.class},
        mergeMode = MergeMode.REPLACE_DEFAULTS)
class HealthEndpointContractTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    void healthEndpointIsPublicAndReportsApplicationStatus() throws Exception {
        assertHealthy("/actuator/health");
    }

    @Test
    void livenessProbeIsPublicAndReportsApplicationStatus() throws Exception {
        assertHealthy("/actuator/health/liveness");
    }

    @Test
    void readinessProbeIsPublicAndReportsApplicationStatus() throws Exception {
        assertHealthy("/actuator/health/readiness");
    }

    private void assertHealthy(String path) throws Exception {
        mockMvc.perform(get(path))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"));
    }
}
