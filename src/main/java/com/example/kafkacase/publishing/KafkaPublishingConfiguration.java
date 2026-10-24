package com.example.kafkacase.publishing;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(KafkaPublishingProperties.class)
public class KafkaPublishingConfiguration {
}
