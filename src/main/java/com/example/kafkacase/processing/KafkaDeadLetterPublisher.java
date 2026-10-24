package com.example.kafkacase.processing;

import com.example.kafkacase.publishing.KafkaPublishingProperties;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class KafkaDeadLetterPublisher implements DeadLetterPublisher {
    private final KafkaTemplate<String, DeadLetterMessage> kafkaTemplate;
    private final DeadLetterProperties deadLetterProperties;
    private final KafkaPublishingProperties publishingProperties;

    public KafkaDeadLetterPublisher(KafkaTemplate<String, DeadLetterMessage> kafkaTemplate,
                                    DeadLetterProperties deadLetterProperties,
                                    KafkaPublishingProperties publishingProperties) {
        this.kafkaTemplate = kafkaTemplate;
        this.deadLetterProperties = deadLetterProperties;
        this.publishingProperties = publishingProperties;
    }

    @Override
    public void publish(DeadLetterMessage message) {
        try {
            String key = message.event() == null ? message.rawRecord().key() : message.event().messageId().toString();
            kafkaTemplate.send(deadLetterProperties.topic(), key, message)
                    .get(publishingProperties.ackTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new DeadLetterPublicationException("Interrupted while publishing a dead-letter message", interrupted);
        } catch (ExecutionException | TimeoutException failure) {
            throw new DeadLetterPublicationException("Kafka did not confirm dead-letter publication", failure);
        }
    }
}
