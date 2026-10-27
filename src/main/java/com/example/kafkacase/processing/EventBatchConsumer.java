package com.example.kafkacase.processing;

import com.example.kafkacase.event.Event;
import com.example.kafkacase.processing.persistence.ProcessedMessageService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.SerializationUtils;
import org.springframework.core.log.LogAccessor;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.List;

@Component
public class EventBatchConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventBatchConsumer.class);
    private static final LogAccessor DESERIALIZATION_LOGGER = new LogAccessor(EventBatchConsumer.class);

    private final ProcessedMessageService processedMessageService;
    private final DeadLetterPublisher deadLetterPublisher;
    private final ProcessingRetryProperties retryProperties;
    private final Clock clock;

    public EventBatchConsumer(ProcessedMessageService processedMessageService,
                              DeadLetterPublisher deadLetterPublisher,
                              ProcessingRetryProperties retryProperties,
                              Clock clock) {
        this.processedMessageService = processedMessageService;
        this.deadLetterPublisher = deadLetterPublisher;
        this.retryProperties = retryProperties;
        this.clock = clock;
    }

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void processBatch(List<ConsumerRecord<String, Event>> records, Acknowledgment acknowledgment) {
        RuntimeException batchFailure = null;

        for (ConsumerRecord<String, Event> record : records) {
            // Deserialization failures retain the original bytes in a trusted Kafka header.
            DeserializationException deserializationFailure = SerializationUtils.getExceptionFromHeader(record,
                    SerializationUtils.VALUE_DESERIALIZER_EXCEPTION_HEADER, DESERIALIZATION_LOGGER);
            RuntimeException processingFailure;
            int attempts;

            // Invalid values go directly to the DLQ; only valid Events receive processing retries.
            if (deserializationFailure != null) {
                processingFailure = deserializationFailure;
                attempts = 1;
            } else if (record.value() == null) {
                processingFailure = new IllegalArgumentException("Kafka record contains no Event value");
                attempts = 1;
            } else {
                processingFailure = processWithRetries(record.value());
                attempts = retryProperties.maxAttempts();
            }

            if (processingFailure == null) {
                continue;
            }

            LOGGER.warn("Record {}-{}@{} failed after {} attempts ({}): {}",
                    record.topic(), record.partition(), record.offset(), attempts,
                    processingFailure.getClass().getSimpleName(), failureReason(processingFailure));

            try {
                DeadLetterMessage message = deadLetterMessage(record, processingFailure, attempts, deserializationFailure);
                deadLetterPublisher.publish(message);
            } catch (RuntimeException publicationFailure) {
                // Preparation and publication failures leave the batch unconfirmed without blocking siblings.
                LOGGER.error("Could not publish record {}-{}@{} to the dead-letter topic; batch will be replayed",
                        record.topic(), record.partition(), record.offset(), publicationFailure);

                if (batchFailure == null) {
                    batchFailure = publicationFailure;
                } else if (batchFailure != publicationFailure) {
                    batchFailure.addSuppressed(publicationFailure);
                }
            }
        }

        // Replay the batch if any DLQ publication remains unconfirmed.
        if (batchFailure != null) {
            throw batchFailure;
        }

        // Acknowledge only when every Event was persisted or its DLQ publication was confirmed.
        acknowledgment.acknowledge();
    }

    private RuntimeException processWithRetries(Event event) {
        for (int attempt = 1; ; attempt++) {
            try {
                processedMessageService.process(event);
                return null;
            } catch (RuntimeException failure) {
                if (attempt >= retryProperties.maxAttempts()) {
                    return failure;
                }

                pauseBeforeRetry();
            }
        }
    }

    private DeadLetterMessage deadLetterMessage(ConsumerRecord<String, Event> record, RuntimeException failure,
                                                int attempts, DeserializationException deserializationFailure) {
        // A tombstone has no bytes; a deserialization failure preserves its original Kafka value.
        DeadLetterMessage.RawRecord rawRecord = record.value() == null
                ? new DeadLetterMessage.RawRecord(record.key(),
                        deserializationFailure == null ? null : deserializationFailure.getData())
                : null;

        return new DeadLetterMessage(record.value(), new DeadLetterMessage.FailureMetadata(
                attempts,
                failure.getClass().getName(),
                failureReason(failure),
                clock.instant(),
                record.topic(),
                record.partition(),
                record.offset()), rawRecord);
    }

    private String failureReason(RuntimeException failure) {
        return failure.getMessage() == null ? failure.getClass().getSimpleName() : failure.getMessage();
    }

    private void pauseBeforeRetry() {
        var backoff = retryProperties.backoff();
        if (backoff.isZero()) {
            return;
        }

        try {
            Thread.sleep(backoff.toMillis(), backoff.toNanosPart() % 1_000_000);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting before retrying an Event", interrupted);
        }
    }
}
