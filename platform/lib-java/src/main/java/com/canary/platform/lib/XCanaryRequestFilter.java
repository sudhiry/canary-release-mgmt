package com.canary.platform.lib;

import com.canary.platform.lib.observability.CanaryMetrics;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.Ordered;

import java.io.IOException;
import java.time.Duration;

public class XCanaryRequestFilter implements Filter, Ordered {

    private final CanaryMetrics metrics;

    public XCanaryRequestFilter(CanaryMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 100;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        boolean canary = false;
        String target = "unknown";
        if (request instanceof HttpServletRequest http) {
            String header = http.getHeader(XCanaryConstants.HEADER_NAME);
            canary = XCanaryConstants.TRUE_VALUE.equals(header);
            target = http.getMethod() + " " + http.getRequestURI();
        }
        boolean prior = XCanaryContext.isCanary();
        XCanaryContext.set(canary);
        long startNanos = System.nanoTime();
        try {
            chain.doFilter(request, response);
        } finally {
            Duration elapsed = Duration.ofNanos(System.nanoTime() - startNanos);
            String outcome = classify(response);
            try {
                metrics.recordHttp(target, outcome, elapsed);
            } finally {
                XCanaryContext.set(prior);
                if (!prior) {
                    XCanaryContext.clear();
                }
            }
        }
    }

    private static String classify(ServletResponse response) {
        if (response instanceof HttpServletResponse http) {
            int status = http.getStatus();
            if (status >= 500) return "server_error";
            if (status >= 400) return "client_error";
        }
        return "success";
    }
}
