package com.canary.platform.lib.observability;

import io.opentelemetry.api.trace.Span;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.Ordered;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

public class CanaryHttpSpanFilter extends OncePerRequestFilter implements Ordered {

    private final String serviceName;

    public CanaryHttpSpanFilter(String serviceName) {
        this.serviceName = serviceName;
    }

    @Override
    public int getOrder() {
        // After XCanaryRequestFilter (HIGHEST_PRECEDENCE+100) so XCanaryContext is set.
        return Ordered.HIGHEST_PRECEDENCE + 200;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        Span span = Span.current();
        span.setAttribute("canary.lane", CanaryLaneTag.current());
        span.setAttribute("canary.service", serviceName);
        chain.doFilter(request, response);
    }
}
