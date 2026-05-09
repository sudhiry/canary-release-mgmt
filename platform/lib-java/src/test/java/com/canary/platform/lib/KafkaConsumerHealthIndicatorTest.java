package com.canary.platform.lib;

import org.junit.jupiter.api.Test;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaConsumerHealthIndicatorTest {

    @Test
    void initiallyOutOfService() {
        KafkaConsumerHealthIndicator h = new KafkaConsumerHealthIndicator(30_000);
        Health health = h.health();
        assertEquals(Status.OUT_OF_SERVICE, health.getStatus());
        assertTrue(health.getDetails().toString().toLowerCase().contains("no poll"));
    }

    @Test
    void upAfterRecentPoll() {
        KafkaConsumerHealthIndicator h = new KafkaConsumerHealthIndicator(30_000);
        h.recordPoll();
        assertEquals(Status.UP, h.health().getStatus());
    }

    @Test
    void outOfServiceWhenStale() {
        KafkaConsumerHealthIndicator h = new KafkaConsumerHealthIndicator(100); // 100ms timeout
        h.recordPoll();
        try { Thread.sleep(200); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        Health health = h.health();
        assertEquals(Status.OUT_OF_SERVICE, health.getStatus());
        assertTrue(health.getDetails().containsKey("staleSeconds"));
    }
}
