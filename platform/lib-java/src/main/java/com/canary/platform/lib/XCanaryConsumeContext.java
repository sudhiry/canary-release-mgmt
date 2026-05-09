package com.canary.platform.lib;

import org.apache.kafka.common.header.Headers;

/**
 * Helper for Kafka consume callbacks: opens an XCanaryContext frame
 * around the handler so outbound HTTP/Kafka/Restate calls inherit
 * x-canary via the existing Phase 1 interceptors.
 */
public final class XCanaryConsumeContext {

    private XCanaryConsumeContext() {}

    public static void runWith(Headers kafkaHeaders, Runnable handler) {
        boolean canary = XCanaryConsumeFilter.isCanaryFlagged(kafkaHeaders);
        boolean prior = XCanaryContext.isCanary();
        XCanaryContext.set(canary);
        try {
            handler.run();
        } finally {
            XCanaryContext.set(prior);
            if (!prior) XCanaryContext.clear();
        }
    }
}
