package com.canary.inventory.kafka;

import com.canary.inventory.store.ConsumedEventStore;
import com.canary.platform.lib.KafkaConsumerHealthIndicator;
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

class InventoryKafkaListenerGatingTest {

    // No Kafka infrastructure is wired; onMessage is called directly.
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withUserConfiguration(TestStubs.class, InventoryKafkaListener.class);

    @Test
    void whenFlagTrueThenListenerIsRegistered() {
        runner.withPropertyValues("app.kafka.consumers.enabled=true")
            .run(ctx -> assertThat(ctx).hasSingleBean(InventoryKafkaListener.class));
    }

    @Test
    void whenFlagFalseThenListenerIsAbsent() {
        runner.withPropertyValues("app.kafka.consumers.enabled=false")
            .run(ctx -> assertThat(ctx).doesNotHaveBean(InventoryKafkaListener.class));
    }

    @Test
    void whenFlagAbsentThenDefaultsToActive() {
        runner.run(ctx -> assertThat(ctx).hasSingleBean(InventoryKafkaListener.class));
    }

    @Test
    void recordPollIsCalledOnEveryMessage() {
        runner.run(ctx -> {
            InventoryKafkaListener listener = ctx.getBean(InventoryKafkaListener.class);
            AtomicBoolean polled = ctx.getBean("pollFlag", AtomicBoolean.class);
            polled.set(false);
            listener.onMessage(record("orders.events", "k", "v", false));
            assertThat(polled.get()).isTrue();
        });
    }

    @Test
    void filterRejectionShortCircuits() {
        runner.run(ctx -> {
            InventoryKafkaListener listener = ctx.getBean(InventoryKafkaListener.class);
            ConsumedEventStore store = ctx.getBean(ConsumedEventStore.class);
            AtomicBoolean shouldProcess = ctx.getBean("shouldProcessFlag", AtomicBoolean.class);
            AtomicBoolean polled = ctx.getBean("pollFlag", AtomicBoolean.class);
            shouldProcess.set(false);
            polled.set(false);
            int before = store.all().size();
            listener.onMessage(record("orders.events", "k", "v", true));
            assertThat(store.all().size()).isEqualTo(before);
            // recordPoll fired BEFORE filter rejected (ordering invariant for readiness gating)
            assertThat(polled.get()).isTrue();
        });
    }

    @Test
    void filterAcceptStoresEvent() {
        runner.run(ctx -> {
            InventoryKafkaListener listener = ctx.getBean(InventoryKafkaListener.class);
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
            InventoryKafkaListener listener = ctx.getBean(InventoryKafkaListener.class);
            AtomicBoolean shouldProcess = ctx.getBean("shouldProcessFlag", AtomicBoolean.class);
            shouldProcess.set(true);
            listener.onMessage(record("orders.events", "k2", "v2", true));
            ConsumedEventStore store = ctx.getBean(ConsumedEventStore.class);
            var last = store.all().get(store.all().size() - 1);
            assertThat(last.headers().get("x-canary")).isEqualTo("true");
        });
    }

    private static ConsumerRecord<String, String> record(String topic, String key, String value, boolean canary) {
        RecordHeaders headers = new RecordHeaders();
        if (canary) headers.add(new RecordHeader("x-canary", "true".getBytes(StandardCharsets.UTF_8)));
        return new ConsumerRecord<>(topic, 0, 0L, 0L,
                TimestampType.NO_TIMESTAMP_TYPE, -1, -1, key, value, headers, Optional.empty());
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
        AtomicBoolean pollFlag() {
            return new AtomicBoolean(false);
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

        @Bean
        KafkaConsumerHealthIndicator kafkaConsumerHealthIndicator(AtomicBoolean pollFlag) {
            return new KafkaConsumerHealthIndicator(30000) {
                @Override
                public void recordPoll() {
                    pollFlag.set(true);
                }
            };
        }
    }
}
