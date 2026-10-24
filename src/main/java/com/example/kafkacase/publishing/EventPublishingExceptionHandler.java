package com.example.kafkacase.publishing;

import com.example.kafkacase.processing.MessageContentConflictException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class EventPublishingExceptionHandler {
    @ExceptionHandler(MessageContentConflictException.class)
    public ProblemDetail conflictingMessageId(MessageContentConflictException exception) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, exception.getMessage());
    }

    @ExceptionHandler(EventPublicationException.class)
    public ProblemDetail handlePublicationFailure(EventPublicationException exception) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE,
                "The event could not be acknowledged by the message broker");
    }
}
