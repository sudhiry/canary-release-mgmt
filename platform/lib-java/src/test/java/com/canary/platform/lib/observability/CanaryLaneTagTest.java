package com.canary.platform.lib.observability;

import com.canary.platform.lib.XCanaryContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CanaryLaneTagTest {

    @AfterEach
    void cleanContext() {
        XCanaryContext.clear();
    }

    @Test
    void currentReturnsStableWhenContextIsNotCanary() {
        XCanaryContext.set(false);
        assertThat(CanaryLaneTag.current()).isEqualTo("stable");
    }

    @Test
    void currentReturnsCanaryWhenContextIsCanary() {
        XCanaryContext.set(true);
        assertThat(CanaryLaneTag.current()).isEqualTo("canary");
    }

    @Test
    void currentDefaultsToStableWhenContextNotSet() {
        XCanaryContext.clear();
        assertThat(CanaryLaneTag.current()).isEqualTo("stable");
    }
}
