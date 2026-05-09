package com.canary.platform.lib;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Request-scoped accumulator of x-served-chain tokens. Each downstream HTTP
 * call adds its (and its transitive callees') tokens here via the response
 * interceptor; the response filter reads + clears at the end.
 */
public final class XServedChainContext {

    public static final String HEADER_NAME = "x-served-chain";

    private XServedChainContext() {}

    private static final ThreadLocal<List<String>> TOKENS = ThreadLocal.withInitial(ArrayList::new);

    public static void append(String token) {
        if (token != null && !token.isBlank()) {
            TOKENS.get().add(token.trim());
        }
    }

    public static void appendChain(String chainCsv) {
        if (chainCsv == null || chainCsv.isBlank()) return;
        for (String t : chainCsv.split(",")) {
            append(t);
        }
    }

    public static List<String> tokens() {
        return Collections.unmodifiableList(TOKENS.get());
    }

    public static void clear() {
        TOKENS.remove();
    }
}
