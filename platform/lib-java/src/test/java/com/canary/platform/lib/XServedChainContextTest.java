package com.canary.platform.lib;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class XServedChainContextTest {
    @AfterEach
    void clear() { XServedChainContext.clear(); }

    @Test
    void initiallyEmpty() {
        assertTrue(XServedChainContext.tokens().isEmpty());
    }

    @Test
    void appendCollectsTokensInOrder() {
        XServedChainContext.append("inventory-service=canary");
        XServedChainContext.append("audit-service=stable");
        assertEquals(List.of("inventory-service=canary", "audit-service=stable"),
                     XServedChainContext.tokens());
    }

    @Test
    void appendChainSplitsCsv() {
        XServedChainContext.appendChain("inventory-service=canary,audit-service=stable");
        assertEquals(2, XServedChainContext.tokens().size());
    }

    @Test
    void appendChainIgnoresBlankAndNull() {
        XServedChainContext.appendChain(null);
        XServedChainContext.appendChain("");
        XServedChainContext.appendChain("   ");
        assertTrue(XServedChainContext.tokens().isEmpty());
    }

    @Test
    void clearResets() {
        XServedChainContext.append("a=b");
        XServedChainContext.clear();
        assertTrue(XServedChainContext.tokens().isEmpty());
    }
}
