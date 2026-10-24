package com.example.kafkacase.publishing;

import com.example.kafkacase.event.Event;
import com.example.kafkacase.processing.ProcessedMessageService;
import org.apache.kafka.common.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class EventPublishingService {
    private final KafkaTemplate<String, Event> kafkaTemplate;
    private final KafkaPublishingProperties properties;
    private final ProcessedMessageService processedMessageService;

    public EventPublishingService(KafkaTemplate<String, Event> kafkaTemplate, KafkaPublishingProperties properties,
                                  ProcessedMessageService processedMessageService) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
        this.processedMessageService = processedMessageService;
    }

    public UUID publish(Event event) {
        if (!processedMessageService.registerForPublication(event)) {
            return event.messageId();
        }

        try {
            kafkaTemplate.send(properties.topic(), event.messageId().toString(), event)
                    .get(properties.ackTimeout().toMillis(), TimeUnit.MILLISECONDS);
            return event.messageId();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new EventPublicationException("Publishing the event was interrupted", exception);
        } catch (ExecutionException | TimeoutException | KafkaException exception) {
            throw new EventPublicationException("Kafka did not acknowledge the event", exception);
        }
    }
}
