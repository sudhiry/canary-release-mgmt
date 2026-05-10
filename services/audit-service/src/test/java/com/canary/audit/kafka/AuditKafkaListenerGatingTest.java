package com.canary.audit.kafka;

import com.canary.audit.store.ConsumedEventStore;
import com.canary.platform.lib.XCanaryConsumeFilter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class AuditKafkaListenerGatingTest {

    // No Kafka infrastructure is wired; onMessage is called directly.
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

    @Test
    void filterRejectionShortCircuits() {
        runner.run(ctx -> {
            AuditKafkaListener listener = ctx.getBean(AuditKafkaListener.class);
            ConsumedEventStore store = ctx.getBean(ConsumedEventStore.class);
            AtomicBoolean shouldProcess = ctx.getBean("shouldProcessFlag", AtomicBoolean.class);
            shouldProcess.set(false);
            int before = store.all().size();
            listener.onMessage(record("orders.events", "k", "v", true));
            assertThat(store.all().size()).isEqualTo(before);
        });
    }

    @Test
    void filterAcceptStoresEvent() {
        runner.run(ctx -> {
            AuditKafkaListener listener = ctx.getBean(AuditKafkaListener.class);
            ConsumedEventStore store = ctx.getBean(ConsumedEventStore.class);
            AtomicBoolean shouldProcess = ctx.getBean("shouldProcessFlag", AtomicBoolean.class);
            shouldProcess.set(true);
            int before = store.all().size();
            listener.onMessage(record("orders.events", "k1", "v1", true));
            assertThat(store.all().size()).isEqualTo(before + 1);
        });
    }

    @Test
    void canaryHeaderIsPersistedToStoredEvent() {
        runner.run(ctx -> {
            AuditKafkaListener listener = ctx.getBean(AuditKafkaListener.class);
            AtomicBoolean shouldProcess = ctx.getBean("shouldProcessFlag", AtomicBoolean.class);
            shouldProcess.set(true);
            listener.onMessage(record("orders.events", "k2", "v2", true));
            // Verify last recorded event captured x-canary=true header
            ConsumedEventStore store = ctx.getBean(ConsumedEventStore.class);
            var last = store.all().get(store.all().size() - 1);
            assertThat(last.headers().get("x-canary")).isEqualTo("true");
        });
    }

    private static ConsumerRecord<String, String> record(String topic, String key, String value, boolean canary) {
        RecordHeaders headers = new RecordHeaders();
        if (canary) headers.add(new RecordHeader("x-canary", "true".getBytes(StandardCharsets.UTF_8)));
        return new ConsumerRecord<>(topic, 0, 0L, 0L,
                TimestampType.NO_TIMESTAMP_TYPE,
                -1, -1, key, value, headers, Optional.empty());
    }

    @Configuration
    static class TestStubs {
        @Bean
        ConsumedEventStore consumedEventStore() {
            return new ConsumedEventStore();
        }

        @Bean
        AtomicBoolean shouldProcessFlag() {
            return new AtomicBoolean(true);
        }

        @Bean
        XCanaryConsumeFilter xCanaryConsumeFilter(AtomicBoolean shouldProcessFlag) {
            return new XCanaryConsumeFilter("stable", () -> false) {
                @Override
                public boolean shouldProcess(org.apache.kafka.common.header.Headers headers) {
                    return shouldProcessFlag.get();
                }
            };
        }
    }
}
