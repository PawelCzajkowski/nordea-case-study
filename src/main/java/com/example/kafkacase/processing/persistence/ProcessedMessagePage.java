package com.example.kafkacase.processing.persistence;

import java.util.List;

public record ProcessedMessagePage(List<ProcessedMessage> content, long totalElements) {
}
