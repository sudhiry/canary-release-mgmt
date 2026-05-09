package com.canary.platform.lib;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class XCanaryConsumerGroupIdResolverTest {

    @Test
    void resolvesStableSuffix() {
        XCanaryConsumerGroupIdResolver r = new XCanaryConsumerGroupIdResolver("stable");
        assertEquals("orders-events-consumers-stable", r.resolve("orders-events-consumers"));
    }

    @Test
    void resolvesCanarySuffix() {
        XCanaryConsumerGroupIdResolver r = new XCanaryConsumerGroupIdResolver("canary");
        assertEquals("orders-events-consumers-canary", r.resolve("orders-events-consumers"));
    }

    @Test
    void defaultsToStableWhenVersionNull() {
        XCanaryConsumerGroupIdResolver r = new XCanaryConsumerGroupIdResolver(null);
        assertEquals("base-stable", r.resolve("base"));
    }

    @Test
    void defaultsToStableWhenVersionBlank() {
        XCanaryConsumerGroupIdResolver r = new XCanaryConsumerGroupIdResolver("   ");
        assertEquals("base-stable", r.resolve("base"));
    }

    @Test
    void rejectsBlankBase() {
        XCanaryConsumerGroupIdResolver r = new XCanaryConsumerGroupIdResolver("stable");
        assertThrows(IllegalArgumentException.class, () -> r.resolve(""));
        assertThrows(IllegalArgumentException.class, () -> r.resolve(null));
    }
}
