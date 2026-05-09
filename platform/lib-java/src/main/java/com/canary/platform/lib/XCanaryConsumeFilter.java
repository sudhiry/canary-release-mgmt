package com.canary.platform.lib;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;
import java.util.function.BooleanSupplier;

/**
 * Per-message filter applied at consume time.
 *   Canary subset: process if x-canary == "true". Skip otherwise.
 *   Stable subset: process if x-canary != "true" OR canary is not ready.
 */
public class XCanaryConsumeFilter {

    private final String ownVersion;
    private final BooleanSupplier canaryReady;

    public XCanaryConsumeFilter(String ownVersion, BooleanSupplier canaryReady) {
        this.ownVersion = (ownVersion == null || ownVersion.isBlank()) ? "stable" : ownVersion.trim();
        this.canaryReady = canaryReady;
    }

    public boolean shouldProcess(Headers kafkaHeaders) {
        boolean carriesCanary = isCanaryFlagged(kafkaHeaders);
        if ("canary".equals(ownVersion)) {
            return carriesCanary;
        }
        return !carriesCanary || !canaryReady.getAsBoolean();
    }

    static boolean isCanaryFlagged(Headers kafkaHeaders) {
        if (kafkaHeaders == null) return false;
        Header h = kafkaHeaders.lastHeader(XCanaryConstants.HEADER_NAME);
        if (h == null || h.value() == null) return false;
        String value = new String(h.value(), StandardCharsets.UTF_8);
        return XCanaryConstants.TRUE_VALUE.equals(value);
    }
}
