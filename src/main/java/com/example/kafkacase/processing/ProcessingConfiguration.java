package com.example.kafkacase.processing;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.time.Clock;

@Configuration
@EnableScheduling
@EnableConfigurationProperties(ProcessedMessageRetentionProperties.class)
public class ProcessingConfiguration {
    @Bean
    Clock systemClock() {
        return Clock.systemUTC();
    }
}
