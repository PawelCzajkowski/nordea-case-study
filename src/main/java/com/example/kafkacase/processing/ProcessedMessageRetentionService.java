package com.example.kafkacase.processing;

import com.example.kafkacase.processing.persistence.ProcessedMessageService;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class ProcessedMessageRetentionService {
    private final ProcessedMessageService processedMessageService;

    public ProcessedMessageRetentionService(ProcessedMessageService processedMessageService) {
        this.processedMessageService = processedMessageService;
    }

    @Scheduled(fixedDelayString = "${app.processing.retention-cleanup-interval}")
    public void deleteExpiredMessages() {
        processedMessageService.deleteExpiredMessages();
    }
}
