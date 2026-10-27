package com.example.kafkacase.processing;

import com.example.kafkacase.processing.persistence.ProcessedMessageService;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@Validated
@RestController
@RequestMapping("/api/v1/messages")
public class ProcessedMessageController {
    private final ProcessedMessageService processedMessageService;

    public ProcessedMessageController(ProcessedMessageService processedMessageService) {
        this.processedMessageService = processedMessageService;
    }

    @GetMapping
    public ProcessedMessagePageResponse list(
            @RequestParam(defaultValue = "0") @Min(0) int page,
            @RequestParam(defaultValue = "50") @Min(1) @Max(100) int size
    ) {
        return ProcessedMessagePageResponse.from(processedMessageService.findPage(page, size), page, size);
    }
}
