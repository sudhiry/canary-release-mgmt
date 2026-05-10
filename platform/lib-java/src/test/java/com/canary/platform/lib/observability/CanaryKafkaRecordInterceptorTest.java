package com.canary.platform.lib.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

class CanaryKafkaRecordInterceptorTest {

    private SimpleMeterRegistry registry;
    private CanaryMetrics metrics;
    private CanaryKafkaRecordInterceptor<Object, Object> interceptor;
    private Consumer<Object, Object> consumer;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new CanaryMetrics(registry, "audit");
        interceptor = new CanaryKafkaRecordInterceptor<>(metrics);
        consumer = Mockito.mock(Consumer.class);
    }

    @Test
    void successPathIncrementsCounterWithSuccessOutcome() {
        ConsumerRecord<Object, Object> rec = new ConsumerRecord<>("payments.charged", 0, 0L, "k", "v");
        interceptor.intercept(rec, consumer);
        interceptor.success(rec, consumer);

        Counter c = registry.find("canary_request_total")
                .tags("substrate", "kafka", "service", "audit", "outcome", "success", "target", "payments.charged")
                .counter();
        assertThat(c).isNotNull();
        assertThat(c.count()).isEqualTo(1.0);
    }

    @Test
    void failurePathIncrementsCounterWithServerErrorOutcome() {
        ConsumerRecord<Object, Object> rec = new ConsumerRecord<>("payments.charged", 0, 0L, "k", "v");
        interceptor.intercept(rec, consumer);
        interceptor.failure(rec, new RuntimeException("boom"), consumer);

        Counter c = registry.find("canary_request_total")
                .tags("substrate", "kafka", "service", "audit", "outcome", "server_error", "target", "payments.charged")
                .counter();
        assertThat(c).isNotNull();
        assertThat(c.count()).isEqualTo(1.0);
    }

    @Test
    void interceptReturnsRecordUnchanged() {
        ConsumerRecord<Object, Object> rec = new ConsumerRecord<>("t", 0, 0L, "k", "v");
        ConsumerRecord<Object, Object> result = interceptor.intercept(rec, consumer);
        assertThat(result).isSameAs(rec);
    }
}
