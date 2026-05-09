package com.canary.payment.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring Boot 4.0 ships Jackson 3 (tools.jackson.databind.ObjectMapper) and only
 * auto-configures a bean of that type. Service code in 1.3.a still uses the
 * Jackson 2 ObjectMapper (com.fasterxml.jackson.databind.ObjectMapper) — both
 * jars are on the classpath, so compile is fine, but no bean of the legacy type
 * exists at runtime. This config registers one so the existing service code
 * keeps working without a Jackson-2-to-3 migration.
 */
@Configuration
public class JacksonConfig {

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}
