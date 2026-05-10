package com.canary.platform.lib;

import org.junit.jupiter.api.Test;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;

import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaConsumerHealthIndicatorTest {

    private AtomicReference<OptionalLong> ageRef(OptionalLong initial) {
        return new AtomicReference<>(initial);
    }

    @Test
    void notAssignedReturnsDown() {
        AtomicReference<OptionalLong> age = ageRef(OptionalLong.of(1000));
        KafkaConsumerHealthIndicator h = new KafkaConsumerHealthIndicator(15_000, age::get);
        Health health = h.health();
        assertEquals(Status.OUT_OF_SERVICE, health.getStatus());
        assertTrue(health.getDetails().toString().toLowerCase().contains("no partitions assigned"));
    }

    @Test
    void assignedButNoHeartbeatReturnsDown() {
        AtomicReference<OptionalLong> age = ageRef(OptionalLong.empty());
        KafkaConsumerHealthIndicator h = new KafkaConsumerHealthIndicator(15_000, age::get);
        h.onPartitionsAssigned();
        Health health = h.health();
        assertEquals(Status.OUT_OF_SERVICE, health.getStatus());
        assertTrue(health.getDetails().toString().toLowerCase().contains("no heartbeat yet"));
    }

    @Test
    void assignedAndFreshHeartbeatReturnsUp() {
        AtomicReference<OptionalLong> age = ageRef(OptionalLong.of(1000));
        KafkaConsumerHealthIndicator h = new KafkaConsumerHealthIndicator(15_000, age::get);
        h.onPartitionsAssigned();
        Health health = h.health();
        assertEquals(Status.UP, health.getStatus());
        assertTrue(health.getDetails().containsKey("heartbeatAgeMs"));
    }

    @Test
    void assignedAndStaleHeartbeatReturnsDown() {
        AtomicReference<OptionalLong> age = ageRef(OptionalLong.of(20_000));
        KafkaConsumerHealthIndicator h = new KafkaConsumerHealthIndicator(15_000, age::get);
        h.onPartitionsAssigned();
        Health health = h.health();
        assertEquals(Status.OUT_OF_SERVICE, health.getStatus());
        assertTrue(health.getDetails().containsKey("heartbeatStaleMs"));
    }

    @Test
    void revokedAfterAssignedReturnsDown() {
        AtomicReference<OptionalLong> age = ageRef(OptionalLong.of(1000));
        KafkaConsumerHealthIndicator h = new KafkaConsumerHealthIndicator(15_000, age::get);
        h.onPartitionsAssigned();
        assertEquals(Status.UP, h.health().getStatus());
        h.onPartitionsRevoked();
        assertEquals(Status.OUT_OF_SERVICE, h.health().getStatus());
    }
}
