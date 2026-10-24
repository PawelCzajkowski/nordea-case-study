package com.example.kafkacase.processing;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

@Configuration
public class ProcessingConfiguration {
    @Bean
    Clock systemClock() {
        return Clock.systemUTC();
    }
}
