package com.canary.platform.lib;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class XCanaryConsumeFilterTest {

    private Headers headers(String value) {
        Headers h = new RecordHeaders();
        if (value != null) {
            h.add("x-canary", value.getBytes(StandardCharsets.UTF_8));
        }
        return h;
    }

    @Test
    void canarySubset_processesCanaryFlagged() {
        XCanaryConsumeFilter f = new XCanaryConsumeFilter("canary", () -> true);
        assertTrue(f.shouldProcess(headers("true")));
    }

    @Test
    void canarySubset_skipsNonCanary() {
        XCanaryConsumeFilter f = new XCanaryConsumeFilter("canary", () -> true);
        assertFalse(f.shouldProcess(headers(null)));
        assertFalse(f.shouldProcess(headers("false")));
    }

    @Test
    void stableSubset_processesNonCanary() {
        XCanaryConsumeFilter f = new XCanaryConsumeFilter("stable", () -> true);
        assertTrue(f.shouldProcess(headers(null)));
        assertTrue(f.shouldProcess(headers("false")));
    }

    @Test
    void stableSubset_skipsCanaryWhenCanaryReady() {
        XCanaryConsumeFilter f = new XCanaryConsumeFilter("stable", () -> true);
        assertFalse(f.shouldProcess(headers("true")));
    }

    @Test
    void stableSubset_processesCanaryWhenCanaryAbsent_gracefulFallback() {
        XCanaryConsumeFilter f = new XCanaryConsumeFilter("stable", () -> false);
        assertTrue(f.shouldProcess(headers("true")));
    }

    @Test
    void unknownVersionTreatedAsStable() {
        XCanaryConsumeFilter f = new XCanaryConsumeFilter("v3", () -> true);
        assertTrue(f.shouldProcess(headers(null)));
        assertFalse(f.shouldProcess(headers("true")));
    }
}
