package com.example.kafkacase.processing;

import jakarta.validation.ConstraintViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice(basePackageClasses = ProcessedMessageController.class)
public class ProcessedMessageExceptionHandler {
    @ExceptionHandler(ConstraintViolationException.class)
    ProblemDetail invalidPagination(ConstraintViolationException exception) {
        return ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST,
                "Page must be non-negative and size must be between 1 and 100");
    }
}
