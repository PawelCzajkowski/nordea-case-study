package com.example.kafkacase.processing;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ProcessedMessageRetentionServiceTest {
    private static final Instant NOW = Instant.parse("2026-10-24T10:15:30Z");

    @Test
    void removesProcessedMessagesOlderThanTheConfiguredRetentionPeriod() {
        // Fix time so the expected retention cutoff is deterministic.
        ProcessedMessageStore store = mock(ProcessedMessageStore.class);
        EventIdentityStore identityStore = mock(EventIdentityStore.class);

        ProcessedMessageRetentionService service = new ProcessedMessageRetentionService(
                store,
                identityStore,
                Clock.fixed(NOW, ZoneOffset.UTC),
                new ProcessedMessageRetentionProperties(Duration.ofDays(30)));

        service.deleteExpiredMessages();

        // Apply the same cutoff to Processed messages and their identity records.
        verify(store).deleteReceivedBefore(Instant.parse("2026-09-24T10:15:30Z"));
        verify(identityStore).deleteRegisteredBefore(Instant.parse("2026-09-24T10:15:30Z"));
    }
}
