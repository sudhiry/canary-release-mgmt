package com.canary.platform.lib;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class XCanaryConsumeContextTest {

    @AfterEach
    void clear() {
        XCanaryContext.clear();
    }

    private Headers headers(String value) {
        Headers h = new RecordHeaders();
        if (value != null) {
            h.add("x-canary", value.getBytes(StandardCharsets.UTF_8));
        }
        return h;
    }

    @Test
    void runsHandlerInsideCanaryContextWhenHeaderPresent() {
        AtomicBoolean observed = new AtomicBoolean(false);
        XCanaryConsumeContext.runWith(headers("true"), () -> observed.set(XCanaryContext.isCanary()));
        assertTrue(observed.get());
    }

    @Test
    void runsHandlerInsideStableContextWhenHeaderAbsent() {
        AtomicBoolean observed = new AtomicBoolean(true);
        XCanaryConsumeContext.runWith(headers(null), () -> observed.set(XCanaryContext.isCanary()));
        assertFalse(observed.get());
    }

    @Test
    void clearsContextAfterHandlerReturns() {
        XCanaryConsumeContext.runWith(headers("true"), () -> { /* no-op */ });
        assertFalse(XCanaryContext.isCanary());
    }

    @Test
    void clearsContextAfterHandlerThrows() {
        try {
            XCanaryConsumeContext.runWith(headers("true"), () -> { throw new RuntimeException("boom"); });
        } catch (RuntimeException ignored) { }
        assertFalse(XCanaryContext.isCanary());
    }
}
