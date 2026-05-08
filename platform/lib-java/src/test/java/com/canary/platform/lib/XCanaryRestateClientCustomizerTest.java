package com.canary.platform.lib;

import dev.restate.common.InvocationOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class XCanaryRestateClientCustomizerTest {

    private final XCanaryRestateClientCustomizer customizer = new XCanaryRestateClientCustomizer();

    @AfterEach
    void clearContext() {
        XCanaryContext.clear();
    }

    @Test
    void appliesXCanaryHeaderWhenContextIsCanary() {
        XCanaryContext.set(true);

        InvocationOptions result = customizer.apply(InvocationOptions.builder());

        assertThat(result.getHeaders())
                .containsEntry(XCanaryConstants.HEADER_NAME, XCanaryConstants.TRUE_VALUE);
    }

    @Test
    void doesNotApplyHeaderWhenContextIsFalse() {
        XCanaryContext.set(false);

        InvocationOptions result = customizer.apply(InvocationOptions.builder());

        // getHeaders() returns null when no header was ever added (lazy LinkedHashMap init).
        Map<String, String> headers = result.getHeaders();
        boolean hasCanaryHeader = headers != null && headers.containsKey(XCanaryConstants.HEADER_NAME);
        assertThat(hasCanaryHeader).isFalse();
    }

    @Test
    void doesNotOverwriteExistingXCanaryHeader() {
        XCanaryContext.set(true);

        InvocationOptions.Builder builder =
                InvocationOptions.builder().header(XCanaryConstants.HEADER_NAME, "preset");

        InvocationOptions result = customizer.apply(builder);

        assertThat(result.getHeaders())
                .containsEntry(XCanaryConstants.HEADER_NAME, "preset");
    }
}
