package com.example.kafkacase.processing;

import com.example.kafkacase.event.Event;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class EventBatchConsumer {
    private final ProcessedMessageService processedMessageService;

    public EventBatchConsumer(ProcessedMessageService processedMessageService) {
        this.processedMessageService = processedMessageService;
    }

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void processBatch(List<ConsumerRecord<String, Event>> records, Acknowledgment acknowledgment) {
        for (ConsumerRecord<String, Event> record : records) {
            processedMessageService.process(record.value());
        }
        acknowledgment.acknowledge();
    }
}
