package com.example.kafkacase.processing.persistence;

import com.example.kafkacase.event.Event;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.temporal.ChronoUnit;

@Service
@EnableConfigurationProperties(ProcessedMessageRetentionProperties.class)
public class ProcessedMessageService {
    private final ProcessedMessageStore store;
    private final Clock clock;
    private final EventIdentityService eventIdentityService;
    private final ProcessedMessageRetentionProperties retentionProperties;

    ProcessedMessageService(ProcessedMessageStore store, Clock clock, EventIdentityService eventIdentityService,
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

        ProcessedMessage processed = store.findByMessageId(event.messageId())
                .orElseThrow(() -> new IllegalStateException("Persisted Event could not be read back"));

        if (!matchesStoredEvent(processed.event(), event)) {
            throw new MessageContentConflictException(event.messageId());
        }

        return processed;
    }

    @Transactional
    public boolean registerForPublication(Event event) {
        eventIdentityService.register(event);
        var processed = store.findByMessageId(event.messageId());

        // A reserved identity may still need publication until its Processed message exists.
        if (processed.isEmpty()) {
            return true;
        }

        if (!matchesStoredEvent(processed.orElseThrow().event(), event)) {
            throw new MessageContentConflictException(event.messageId());
        }

        // An identical Event already in the read model does not need another Kafka publication.
        return false;
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

    private boolean matchesStoredEvent(Event stored, Event original) {
        // PostgreSQL/JDBC round timestamps to microseconds; the identity fingerprint retains the original nanoseconds.
        var storedTimestamp = original.occurredAt().plusNanos(500).truncatedTo(ChronoUnit.MICROS);

        return stored.equals(new Event(original.messageId(), original.type(), original.schemaVersion(),
                storedTimestamp, original.payload()));
    }
}
