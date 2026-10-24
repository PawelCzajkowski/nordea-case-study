package com.example.kafkacase.processing;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaListenerErrorConfiguration {
    @Bean
    FixedBackOff kafkaListenerErrorBackOff(ProcessingRetryProperties retryProperties) {
        return new FixedBackOff(retryProperties.backoff().toMillis(), FixedBackOff.UNLIMITED_ATTEMPTS);
    }

    @Bean
    DefaultErrorHandler kafkaListenerErrorHandler(FixedBackOff kafkaListenerErrorBackOff) {
        // A DLQ send that the broker did not confirm must never fall through to Spring's finite default recovery.
        return new DefaultErrorHandler(kafkaListenerErrorBackOff);
    }
}
