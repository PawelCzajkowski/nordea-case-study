package com.example.kafkacase.security;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;

@Configuration
public class SecurityConfiguration {
    @Bean
    SecurityFilterChain applicationSecurity(HttpSecurity http) throws Exception {
        return http
                .csrf(AbstractHttpConfigurer::disable)
                .authorizeHttpRequests(authorize -> authorize
                        // Kubernetes probes need public access to the health groups.
                        .requestMatchers("/actuator/health/**").permitAll()

                        // Publishing and retrieval require separate JWT scopes.
                        .requestMatchers(HttpMethod.POST, "/api/v1/events").hasAuthority("SCOPE_messages.write")
                        .requestMatchers(HttpMethod.GET, "/api/v1/messages").hasAuthority("SCOPE_messages.read")

                        .requestMatchers("/api/**").authenticated()
                        .anyRequest().permitAll())
                .oauth2ResourceServer(resourceServer -> resourceServer.jwt(Customizer.withDefaults()))
                .build();
    }
}
