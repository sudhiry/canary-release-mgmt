package com.canary.platform.lib;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class XCanaryContextTest {

    @AfterEach
    void clearContext() {
        XCanaryContext.clear();
    }

    @Test
    void initiallyUnset() {
        assertThat(XCanaryContext.isCanary()).isFalse();
    }

    @Test
    void setMakesIsCanaryTrue() {
        XCanaryContext.set(true);
        assertThat(XCanaryContext.isCanary()).isTrue();
    }

    @Test
    void clearResetsToFalse() {
        XCanaryContext.set(true);
        XCanaryContext.clear();
        assertThat(XCanaryContext.isCanary()).isFalse();
    }

    @Test
    void runWithCanaryRestoresPriorValueOnExit() {
        XCanaryContext.set(false);
        XCanaryContext.runWithCanary(true, () -> {
            assertThat(XCanaryContext.isCanary()).isTrue();
        });
        assertThat(XCanaryContext.isCanary()).isFalse();
    }

    @Test
    void runWithCanaryRestoresPriorValueEvenOnException() {
        XCanaryContext.set(false);
        try {
            XCanaryContext.runWithCanary(true, () -> {
                throw new RuntimeException("boom");
            });
        } catch (RuntimeException ignored) {
        }
        assertThat(XCanaryContext.isCanary()).isFalse();
    }
}
