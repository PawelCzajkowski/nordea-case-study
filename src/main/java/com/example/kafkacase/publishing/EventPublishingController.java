package com.example.kafkacase.publishing;

import com.example.kafkacase.event.Event;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1/events")
public class EventPublishingController {
    private final EventPublishingService eventPublishingService;

    public EventPublishingController(EventPublishingService eventPublishingService) {
        this.eventPublishingService = eventPublishingService;
    }

    @PostMapping
    public ResponseEntity<PublishEventResponse> publish(@Valid @RequestBody Event event) {
        UUID messageId = eventPublishingService.publish(event);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(new PublishEventResponse(messageId));
    }

    public record PublishEventResponse(UUID messageId) {
    }
}
