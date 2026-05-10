package com.canary.platform.lib;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class KafkaConsumerHealthIndicator implements HealthIndicator {

    private final long heartbeatStaleMs;
    private final Supplier<OptionalLong> lastHeartbeatAgeMsSupplier;
    private final AtomicBoolean assigned = new AtomicBoolean(false);

    public KafkaConsumerHealthIndicator(long heartbeatStaleMs,
                                        Supplier<OptionalLong> lastHeartbeatAgeMsSupplier) {
        this.heartbeatStaleMs = heartbeatStaleMs;
        this.lastHeartbeatAgeMsSupplier = lastHeartbeatAgeMsSupplier;
    }

    public void onPartitionsAssigned() {
        assigned.set(true);
    }

    public void onPartitionsRevoked() {
        assigned.set(false);
    }

    @Override
    public Health health() {
        if (!assigned.get()) {
            return Health.outOfService().withDetail("reason", "no partitions assigned").build();
        }
        OptionalLong ageMs = lastHeartbeatAgeMsSupplier.get();
        if (ageMs.isEmpty()) {
            return Health.outOfService().withDetail("reason", "no heartbeat yet").build();
        }
        long age = ageMs.getAsLong();
        if (age > heartbeatStaleMs) {
            return Health.outOfService().withDetail("heartbeatStaleMs", age).build();
        }
        return Health.up().withDetail("heartbeatAgeMs", age).build();
    }
}
