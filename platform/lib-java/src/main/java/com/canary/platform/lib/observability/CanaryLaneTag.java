package com.canary.platform.lib.observability;

import com.canary.platform.lib.XCanaryContext;

public final class CanaryLaneTag {

    public static final String STABLE = "stable";
    public static final String CANARY = "canary";

    private CanaryLaneTag() {}

    /** Returns the current lane tag value derived from {@link XCanaryContext}. */
    public static String current() {
        return XCanaryContext.isCanary() ? CANARY : STABLE;
    }
}
