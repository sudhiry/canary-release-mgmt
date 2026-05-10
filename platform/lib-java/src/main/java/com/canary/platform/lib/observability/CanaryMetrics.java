package com.canary.platform.lib.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;

public class CanaryMetrics {

    private static final String COUNTER_NAME = "canary_request_total";
    private static final String TIMER_NAME = "canary_request_duration_seconds";
    private static final String SHADOW_NAME = "canary_shadow_mismatch_total";

    private final MeterRegistry registry;
    private final String serviceName;

    public CanaryMetrics(MeterRegistry registry, String serviceName) {
        this.registry = registry;
        this.serviceName = serviceName;
    }

    public void recordHttp(String target, String outcome, Duration duration) {
        record("http", target, outcome, duration);
    }

    public void recordKafka(String target, String outcome, Duration duration) {
        record("kafka", target, outcome, duration);
    }

    public void recordRestate(String target, String outcome, Duration duration) {
        record("restate", target, outcome, duration);
    }

    public void recordShadowMismatch(String field) {
        Counter.builder(SHADOW_NAME)
                .tags("service", serviceName, "field", field)
                .register(registry)
                .increment();
    }

    private void record(String substrate, String target, String outcome, Duration duration) {
        String lane = CanaryLaneTag.current();
        Tags counterTags = Tags.of(
                "substrate", substrate,
                "service", serviceName,
                "lane", lane,
                "outcome", outcome,
                "target", target);
        Counter.builder(COUNTER_NAME).tags(counterTags).register(registry).increment();

        Tags timerTags = Tags.of(
                "substrate", substrate,
                "service", serviceName,
                "lane", lane,
                "target", target);
        Timer.builder(TIMER_NAME)
                .tags(timerTags)
                .publishPercentileHistogram()
                .register(registry)
                .record(duration);
    }
}
