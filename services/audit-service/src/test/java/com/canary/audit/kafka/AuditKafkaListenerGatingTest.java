package com.canary.audit.kafka;

import com.canary.audit.store.ConsumedEventStore;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

class AuditKafkaListenerGatingTest {

    /**
     * Spring Boot 4 has no KafkaAutoConfiguration to load — the gating test only needs
     * to verify that the bean class is filtered in/out by @ConditionalOnProperty. We
     * load just the listener class directly; no Kafka infrastructure is actually wired.
     */
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withUserConfiguration(TestStubs.class, AuditKafkaListener.class);

    @Test
    void whenFlagTrueThenListenerIsRegistered() {
        runner.withPropertyValues("app.kafka.consumers.enabled=true")
            .run(ctx -> assertThat(ctx).hasSingleBean(AuditKafkaListener.class));
    }

    @Test
    void whenFlagFalseThenListenerIsAbsent() {
        runner.withPropertyValues("app.kafka.consumers.enabled=false")
            .run(ctx -> assertThat(ctx).doesNotHaveBean(AuditKafkaListener.class));
    }

    @Test
    void whenFlagAbsentThenDefaultsToActive() {
        runner.run(ctx -> assertThat(ctx).hasSingleBean(AuditKafkaListener.class));
    }

    @Configuration
    static class TestStubs {
        @Bean
        ConsumedEventStore consumedEventStore() {
            return new ConsumedEventStore();
        }
    }
}
