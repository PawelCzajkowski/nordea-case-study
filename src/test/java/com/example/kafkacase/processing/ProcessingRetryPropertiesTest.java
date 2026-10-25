package com.example.kafkacase.processing;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProcessingRetryPropertiesTest {
    @Test
    void retryCountAndBackoffCanBeOverriddenByExternalConfiguration() {
        // Start a minimal context with deployment-specific retry and DLQ settings.
        new ApplicationContextRunner()
                .withUserConfiguration(ProcessingConfiguration.class)
                .withPropertyValues(
                        "app.processing.retention-period=30d",
                        "app.kafka.processing.retry.max-attempts=5",
                        "app.kafka.processing.retry.backoff=3s",
                        "app.kafka.dead-letter.topic=case-events.failures")
                .run(context -> {
                    ProcessingRetryProperties retry = context.getBean(ProcessingRetryProperties.class);
                    DeadLetterProperties deadLetter = context.getBean(DeadLetterProperties.class);

                    // The bound properties must reflect external overrides rather than application defaults.
                    assertEquals(5, retry.maxAttempts());
                    assertEquals(Duration.ofSeconds(3), retry.backoff());
                    assertEquals("case-events.failures", deadLetter.topic());
                });
    }
}
