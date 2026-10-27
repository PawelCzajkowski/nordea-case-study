package com.example.kafkacase.processing.persistence;

import com.example.kafkacase.event.Event;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

@Service
@EnableConfigurationProperties(ProcessedMessageRetentionProperties.class)
public class ProcessedMessageService {
    private final JdbcProcessedMessageStore store;
    private final Clock clock;
    private final EventIdentityService eventIdentityService;
    private final ProcessedMessageRetentionProperties retentionProperties;

    ProcessedMessageService(JdbcProcessedMessageStore store, Clock clock, EventIdentityService eventIdentityService,
                            ProcessedMessageRetentionProperties retentionProperties) {
        this.store = store;
        this.clock = clock;
        this.eventIdentityService = eventIdentityService;
        this.retentionProperties = retentionProperties;
    }

    @Transactional
    public ProcessedMessage process(Event event) {
        // Register identity before persistence so a replay cannot overwrite different Event content.
        eventIdentityService.register(event);
        store.insertIfAbsent(event, clock.instant());

        return findMatchingProcessedMessage(event)
                .orElseThrow(() -> new IllegalStateException("Persisted Event could not be read back"));
    }

    @Transactional
    public boolean registerForPublication(Event event) {
        eventIdentityService.register(event);
        // A reserved identity may still need publication until its Processed message exists.
        return findMatchingProcessedMessage(event).isEmpty();
    }

    @Transactional(readOnly = true)
    public ProcessedMessagePage findPage(int page, int size) {
        return store.findPage(page, size);
    }

    @Transactional
    public void deleteExpiredMessages() {
        var cutoff = clock.instant().minus(retentionProperties.retentionPeriod());

        // Both records expire in one transaction, using their own original timestamps.
        store.deleteReceivedBefore(cutoff);
        eventIdentityService.deleteRegisteredBefore(cutoff);
    }

    private Optional<ProcessedMessage> findMatchingProcessedMessage(Event event) {
        var processed = store.findByMessageId(event.messageId());

        if (processed.isPresent() && !matchesStoredEvent(processed.get().event(), event)) {
            throw new MessageContentConflictException(event.messageId());
        }

        return processed;
    }

    private boolean matchesStoredEvent(Event stored, Event original) {
        // PostgreSQL/JDBC round timestamps to microseconds; the identity fingerprint retains the original nanoseconds.
        var storedTimestamp = original.occurredAt().plusNanos(500).truncatedTo(ChronoUnit.MICROS);

        return stored.equals(new Event(original.messageId(), original.type(), original.schemaVersion(),
                storedTimestamp, original.payload()));
    }
}
