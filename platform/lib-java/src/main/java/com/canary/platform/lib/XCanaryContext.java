package com.canary.platform.lib;

public final class XCanaryContext {

    private static final ThreadLocal<Boolean> FLAG = ThreadLocal.withInitial(() -> Boolean.FALSE);

    private XCanaryContext() {}

    public static boolean isCanary() {
        return Boolean.TRUE.equals(FLAG.get());
    }

    public static void set(boolean canary) {
        FLAG.set(canary);
    }

    public static void clear() {
        FLAG.remove();
    }

    public static void runWithCanary(boolean canary, Runnable body) {
        boolean prior = isCanary();
        FLAG.set(canary);
        try {
            body.run();
        } finally {
            FLAG.set(prior);
        }
    }
}
