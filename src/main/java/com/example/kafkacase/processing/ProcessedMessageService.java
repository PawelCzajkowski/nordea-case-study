package com.example.kafkacase.processing;

import com.example.kafkacase.event.Event;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.temporal.ChronoUnit;

@Service
public class ProcessedMessageService {
    private final ProcessedMessageStore store;
    private final Clock clock;
    private final EventIdentityService eventIdentityService;

    public ProcessedMessageService(ProcessedMessageStore store, Clock clock, EventIdentityService eventIdentityService) {
        this.store = store;
        this.clock = clock;
        this.eventIdentityService = eventIdentityService;
    }

    @Transactional
    public ProcessedMessage process(Event event) {
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
        if (processed.isEmpty()) {
            return true;
        }
        if (!matchesStoredEvent(processed.orElseThrow().event(), event)) {
            throw new MessageContentConflictException(event.messageId());
        }
        return false;
    }

    @Transactional(readOnly = true)
    public ProcessedMessagePage findPage(int page, int size) {
        return store.findPage(page, size);
    }

    private boolean matchesStoredEvent(Event stored, Event original) {
        // PostgreSQL/JDBC round timestamps to microseconds; the identity fingerprint retains the original nanoseconds.
        var storedTimestamp = original.occurredAt().plusNanos(500).truncatedTo(ChronoUnit.MICROS);
        return stored.equals(new Event(original.messageId(), original.type(), original.schemaVersion(),
                storedTimestamp, original.payload()));
    }

}
