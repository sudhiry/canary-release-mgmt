package com.canary.platform.lib.observability;

import com.canary.platform.lib.XCanaryContext;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class CanaryMetricsTest {

    private MeterRegistry registry;
    private CanaryMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new CanaryMetrics(registry, "payment");
    }

    @AfterEach
    void tearDown() {
        XCanaryContext.clear();
    }

    @Test
    void recordHttpIncrementsCounterWithExpectedTags() {
        XCanaryContext.set(false);
        metrics.recordHttp("POST /pay", "success", Duration.ofMillis(42));

        Counter c = registry.find("canary_request_total")
                .tags("substrate", "http", "service", "payment", "lane", "stable",
                      "outcome", "success", "target", "POST /pay")
                .counter();
        assertThat(c).isNotNull();
        assertThat(c.count()).isEqualTo(1.0);
    }

    @Test
    void recordHttpRecordsTimerWithExpectedTags() {
        XCanaryContext.set(true);
        metrics.recordHttp("GET /healthz", "success", Duration.ofMillis(7));

        Timer t = registry.find("canary_request_duration_seconds")
                .tags("substrate", "http", "service", "payment", "lane", "canary",
                      "target", "GET /healthz")
                .timer();
        assertThat(t).isNotNull();
        assertThat(t.count()).isEqualTo(1L);
        assertThat(t.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS)).isEqualTo(7.0);
    }

    @Test
    void recordKafkaUsesKafkaSubstrateAndTopicTarget() {
        XCanaryContext.set(true);
        metrics.recordKafka("payments.charged", "success", Duration.ofMillis(100));

        Counter c = registry.find("canary_request_total")
                .tags("substrate", "kafka", "service", "payment", "lane", "canary",
                      "outcome", "success", "target", "payments.charged")
                .counter();
        assertThat(c).isNotNull();
        assertThat(c.count()).isEqualTo(1.0);
    }

    @Test
    void recordRestateUsesRestateSubstrateAndHandlerTarget() {
        XCanaryContext.set(false);
        metrics.recordRestate("CheckoutSagaStable.run", "server_error", Duration.ofMillis(250));

        Counter c = registry.find("canary_request_total")
                .tags("substrate", "restate", "service", "payment", "lane", "stable",
                      "outcome", "server_error", "target", "CheckoutSagaStable.run")
                .counter();
        assertThat(c).isNotNull();
        assertThat(c.count()).isEqualTo(1.0);
    }

    @Test
    void recordShadowMismatchIncrementsByService() {
        metrics.recordShadowMismatch("totalCents");

        Counter c = registry.find("canary_shadow_mismatch_total")
                .tags("service", "payment", "field", "totalCents")
                .counter();
        assertThat(c).isNotNull();
        assertThat(c.count()).isEqualTo(1.0);
    }
}
