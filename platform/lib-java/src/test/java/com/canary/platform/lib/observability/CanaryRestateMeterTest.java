package com.canary.platform.lib.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CanaryRestateMeterTest {

    private SimpleMeterRegistry registry;
    private CanaryMetrics metrics;
    private CanaryRestateMeter meter;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new CanaryMetrics(registry, "order");
        meter = new CanaryRestateMeter(metrics);
    }

    @Test
    void measureReturnsValueAndRecordsSuccess() throws Exception {
        String result = meter.measure("CheckoutSagaStable.run", () -> "ok");
        assertThat(result).isEqualTo("ok");

        Counter c = registry.find("canary_request_total")
                .tags("substrate", "restate", "outcome", "success", "target", "CheckoutSagaStable.run")
                .counter();
        assertThat(c).isNotNull();
        assertThat(c.count()).isEqualTo(1.0);
    }

    @Test
    void measureRecordsServerErrorWhenBodyThrows() {
        assertThatThrownBy(() -> meter.measure("CheckoutSagaStable.run", () -> {
            throw new RuntimeException("kaboom");
        })).isInstanceOf(RuntimeException.class).hasMessage("kaboom");

        Counter c = registry.find("canary_request_total")
                .tags("substrate", "restate", "outcome", "server_error", "target", "CheckoutSagaStable.run")
                .counter();
        assertThat(c).isNotNull();
        assertThat(c.count()).isEqualTo(1.0);
    }

    @Test
    void measureAlwaysRecordsTimer() throws Exception {
        meter.measure("PaymentVOStable.charge", () -> 1);

        Timer t = registry.find("canary_request_duration_seconds")
                .tags("substrate", "restate", "target", "PaymentVOStable.charge")
                .timer();
        assertThat(t).isNotNull();
        assertThat(t.count()).isEqualTo(1L);
    }
}
