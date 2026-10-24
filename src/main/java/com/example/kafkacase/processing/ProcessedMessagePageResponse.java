package com.example.kafkacase.processing;

import java.util.List;

public record ProcessedMessagePageResponse(
        List<ProcessedMessageResponse> content,
        int page,
        int size,
        long totalElements,
        long totalPages
) {
    public static ProcessedMessagePageResponse from(ProcessedMessagePage processedPage, int page, int size) {
        List<ProcessedMessageResponse> content = processedPage.content().stream()
                .map(ProcessedMessageResponse::from)
                .toList();
        long totalPages = processedPage.totalElements() == 0
                ? 0
                : 1 + (processedPage.totalElements() - 1) / size;
        return new ProcessedMessagePageResponse(content, page, size, processedPage.totalElements(), totalPages);
    }
}
