package com.example.kafkacase.processing;

import com.example.kafkacase.event.Event;
import com.example.kafkacase.publishing.KafkaPublishingProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.SendResult;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaListenerErrorConfigurationTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void retriesBatchUntilDeadLetterPublicationIsConfirmedBeforeAcknowledging(boolean brokerRejectsPublication) {
        var configuration = new KafkaListenerErrorConfiguration();
        var properties = new ProcessingRetryProperties(1, Duration.ZERO);
        var errorHandler = configuration.kafkaListenerErrorHandler(configuration.kafkaListenerErrorBackOff(properties));
        var clock = Clock.fixed(Instant.parse("2026-10-23T12:30:00Z"), ZoneOffset.UTC);
        var event = new Event(UUID.fromString("b395464a-605a-4cb7-928c-c9b5e0ddb9c0"),
                "customer.updated", "1.0", Instant.parse("2026-10-23T12:00:00Z"),
                new ObjectMapper().createObjectNode().put("customerId", "c-123"));
        var record = new ConsumerRecord<>("events", 0, 7L, event.messageId().toString(), event);
        var partition = new TopicPartition(record.topic(), record.partition());
        var records = new ConsumerRecords<>(Map.of(partition, List.of(record)));
        var acknowledgment = mock(Acknowledgment.class);
        var service = mock(ProcessedMessageService.class);
        KafkaTemplate<String, DeadLetterMessage> kafkaTemplate = mock(KafkaTemplate.class);
        var publisher = new KafkaDeadLetterPublisher(kafkaTemplate, new DeadLetterProperties("events.dlq"),
                new KafkaPublishingProperties("events", Duration.ofMillis(1)));
        var listener = new EventBatchConsumer(service, publisher, properties, clock);
        Consumer<String, Event> kafkaConsumer = mock(Consumer.class);
        var container = mock(MessageListenerContainer.class);
        when(kafkaConsumer.assignment()).thenReturn(Set.of(partition));
        when(container.isRunning()).thenReturn(true);
        when(service.process(event)).thenThrow(new IllegalStateException("database unavailable"));

        // Exceed Spring's default finite recovery limit before allowing the DLQ send to succeed.
        int unconfirmedPublications = 15;
        var publicationAttempts = new AtomicInteger();
        when(kafkaTemplate.send(eq("events.dlq"), eq(event.messageId().toString()), any(DeadLetterMessage.class)))
                .thenAnswer(invocation -> {
                    verify(acknowledgment, never()).acknowledge();
                    if (publicationAttempts.incrementAndGet() <= unconfirmedPublications) {
                        return brokerRejectsPublication
                                ? CompletableFuture.<SendResult<String, DeadLetterMessage>>failedFuture(
                                        new IllegalStateException("broker rejected the record"))
                                : new CompletableFuture<SendResult<String, DeadLetterMessage>>();
                    }
                    return CompletableFuture.<SendResult<String, DeadLetterMessage>>completedFuture(null);
                });

        Runnable invokeListener = () -> listener.processBatch(List.of(record), acknowledgment);
        var initialFailure = assertThrows(DeadLetterPublicationException.class, invokeListener::run);

        errorHandler.handleBatch(initialFailure, records, kafkaConsumer, container, invokeListener);

        var messages = ArgumentCaptor.forClass(DeadLetterMessage.class);
        var order = inOrder(kafkaTemplate, acknowledgment);
        order.verify(kafkaTemplate, times(unconfirmedPublications + 1))
                .send(eq("events.dlq"), eq(event.messageId().toString()), messages.capture());
        order.verify(acknowledgment).acknowledge();
        for (DeadLetterMessage message : messages.getAllValues()) {
            assertEquals(event, message.event());
            assertEquals(record.topic(), message.failure().sourceTopic());
            assertEquals(record.partition(), message.failure().sourcePartition());
            assertEquals(record.offset(), message.failure().sourceOffset());
        }
    }
}
