package com.example.kafkacase.processing;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class KafkaListenerErrorConfigurationTest {
    @Test
    void retriesBatchWhenDeadLetterPublicationIsUnconfirmedWithoutGivingUp() {
        var configuration = new KafkaListenerErrorConfiguration();
        var properties = new ProcessingRetryProperties(4, Duration.ofMillis(500));

        FixedBackOff retryBackOff = configuration.kafkaListenerErrorBackOff(properties);

        assertEquals(500L, retryBackOff.getInterval());
        assertEquals(FixedBackOff.UNLIMITED_ATTEMPTS, retryBackOff.getMaxAttempts());
        assertInstanceOf(DefaultErrorHandler.class, configuration.kafkaListenerErrorHandler(retryBackOff));
    }
}
