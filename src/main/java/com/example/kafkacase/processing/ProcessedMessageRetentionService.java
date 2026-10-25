package com.example.kafkacase.processing;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;

@Service
public class ProcessedMessageRetentionService {
    private final ProcessedMessageStore store;
    private final EventIdentityStore identityStore;
    private final Clock clock;
    private final ProcessedMessageRetentionProperties properties;

    public ProcessedMessageRetentionService(
            ProcessedMessageStore store,
            EventIdentityStore identityStore,
            Clock clock,
            ProcessedMessageRetentionProperties properties
    ) {
        this.store = store;
        this.identityStore = identityStore;
        this.clock = clock;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${app.processing.retention-cleanup-interval}")
    @Transactional
    public void deleteExpiredMessages() {
        var cutoff = clock.instant().minus(properties.retentionPeriod());

        // Bound storage and the identity ledger using the same retention window and transaction.
        store.deleteReceivedBefore(cutoff);
        identityStore.deleteRegisteredBefore(cutoff);
    }
}
