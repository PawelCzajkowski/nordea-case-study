package com.example.kafkacase.processing;

import java.util.List;

public record ProcessedMessagePage(List<ProcessedMessage> content, long totalElements) {
}
