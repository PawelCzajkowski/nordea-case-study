package com.example.kafkacase.security;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.security.oauth2.jwt.BadJwtException;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;

import java.time.Instant;

@Configuration(proxyBeanMethods = false)
@Profile("local")
public class LocalJwtConfiguration {
    @Bean
    JwtDecoder localJwtDecoder() {
        return token -> {
            if (!"local-dev-token".equals(token)) {
                throw new BadJwtException("Invalid local development token");
            }

            Instant now = Instant.now();
            return Jwt.withTokenValue(token)
                    .header("alg", "none")
                    .subject("local-developer")
                    .claim("scope", "messages.read messages.write")
                    .issuedAt(now)
                    .expiresAt(now.plusSeconds(3600))
                    .build();
        };
    }
}
