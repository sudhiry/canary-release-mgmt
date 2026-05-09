package com.canary.platform.lib;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

import java.util.concurrent.atomic.AtomicLong;

public class KafkaConsumerHealthIndicator implements HealthIndicator {

    private final long timeoutMs;
    private final AtomicLong lastPollMs = new AtomicLong(0);

    public KafkaConsumerHealthIndicator(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public void recordPoll() {
        lastPollMs.set(System.currentTimeMillis());
    }

    @Override
    public Health health() {
        long last = lastPollMs.get();
        if (last == 0) {
            return Health.outOfService().withDetail("reason", "no poll yet").build();
        }
        long ageMs = System.currentTimeMillis() - last;
        if (ageMs > timeoutMs) {
            return Health.outOfService().withDetail("staleSeconds", ageMs / 1000).build();
        }
        return Health.up().withDetail("ageMs", ageMs).build();
    }
}
