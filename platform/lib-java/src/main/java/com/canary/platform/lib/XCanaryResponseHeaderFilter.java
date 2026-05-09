package com.canary.platform.lib;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.Ordered;

import java.io.IOException;

/**
 * Stamps the configured version on the outbound response so e2e tests can
 * verify which subset (stable | canary) handled the request. Reads the
 * version once at construction.
 */
public class XCanaryResponseHeaderFilter implements Filter, Ordered {

    public static final String HEADER_NAME = "x-served-version";
    public static final String DEFAULT_VERSION = "stable";

    private final String version;

    public XCanaryResponseHeaderFilter(String version) {
        this.version = (version == null || version.isBlank()) ? DEFAULT_VERSION : version.trim();
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 200;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (response instanceof HttpServletResponse http) {
            http.setHeader(HEADER_NAME, version);
        }
        chain.doFilter(request, response);
    }
}
