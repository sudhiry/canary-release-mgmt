package com.canary.platform.lib;

import dev.restate.common.InvocationOptions;

/**
 * Applies the {@code x-canary: true} header to an outbound Restate
 * {@link InvocationOptions.Builder} when the current thread's {@link XCanaryContext}
 * is canary.
 *
 * <p>Usage: wrap your {@link dev.restate.sdk.ServiceHandle} call options:
 * <pre>{@code
 *   InvocationOptions opts = canaryCustomizer.apply(InvocationOptions.builder());
 *   ctx.serviceClient(MyService.class).call(MyService::myHandler, input, opts);
 * }</pre>
 *
 * <p>If the builder already carries an {@code x-canary} header the customizer
 * leaves it untouched (caller wins).
 *
 * <p>Tied to {@code dev.restate:sdk-api:2.7.0} / {@code dev.restate:common:2.7.0}.
 * The relevant API surface is {@link InvocationOptions#builder()},
 * {@link InvocationOptions.Builder#header(String, String)}, and
 * {@link InvocationOptions.Builder#build()}.
 */
public class XCanaryRestateClientCustomizer {

    /**
     * Stamps {@code x-canary: true} onto the builder when the calling thread is in canary
     * context, then builds and returns the resulting {@link InvocationOptions}.
     *
     * <p>If the builder already has an {@code x-canary} entry it is preserved as-is.
     *
     * @param builder a mutable {@link InvocationOptions.Builder}; must not be {@code null}
     * @return a built {@link InvocationOptions} (never {@code null})
     */
    public InvocationOptions apply(InvocationOptions.Builder builder) {
        if (XCanaryContext.isCanary()) {
            // getHeaders() returns null when no header has been set on the builder yet
            // (the LinkedHashMap field is lazily initialised only on the first header() call).
            java.util.Map<String, String> existing = builder.getHeaders();
            if (existing == null || !existing.containsKey(XCanaryConstants.HEADER_NAME)) {
                builder.header(XCanaryConstants.HEADER_NAME, XCanaryConstants.TRUE_VALUE);
            }
        }
        return builder.build();
    }
}
