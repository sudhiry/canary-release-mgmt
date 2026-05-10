package com.canary.platform.lib.observability;

import java.time.Duration;

public class CanaryRestateMeter {

    @FunctionalInterface
    public interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    private final CanaryMetrics metrics;

    public CanaryRestateMeter(CanaryMetrics metrics) {
        this.metrics = metrics;
    }

    public <T> T measure(String handlerName, ThrowingSupplier<T> body) throws Exception {
        long start = System.nanoTime();
        try {
            T result = body.get();
            metrics.recordRestate(handlerName, "success", Duration.ofNanos(System.nanoTime() - start));
            return result;
        } catch (Exception e) {
            metrics.recordRestate(handlerName, "server_error", Duration.ofNanos(System.nanoTime() - start));
            throw e;
        }
    }
}
