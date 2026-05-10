package com.canary.audit;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

// @EnableKafka is required: Spring Boot 4.x's KafkaAutoConfiguration no
// longer auto-imports it (broke between 3.x and 4.x). Without this, the
// KafkaListenerEndpointRegistry never starts the @KafkaListener-backed
// MessageListenerContainer, so the Java service never joins its consumer
// group and never receives messages — silently. Surfaced during cluster
// verification of Phase 2.b: kafka-consumer-groups.sh --list showed only
// the Node service groups; no audit/payment/inventory.
@SpringBootApplication
@EnableKafka
public class AuditApplication {
    public static void main(String[] args) {
        SpringApplication.run(AuditApplication.class, args);
    }
}
