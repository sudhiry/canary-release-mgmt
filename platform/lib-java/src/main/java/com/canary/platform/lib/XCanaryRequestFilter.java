package com.canary.platform.lib;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.core.Ordered;

import java.io.IOException;

public class XCanaryRequestFilter implements Filter, Ordered {

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 100;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        boolean canary = false;
        if (request instanceof HttpServletRequest http) {
            String header = http.getHeader(XCanaryConstants.HEADER_NAME);
            canary = XCanaryConstants.TRUE_VALUE.equals(header);
        }
        boolean prior = XCanaryContext.isCanary();
        XCanaryContext.set(canary);
        try {
            chain.doFilter(request, response);
        } finally {
            XCanaryContext.set(prior);
            // Defensive: if there was no prior (default false) we still want clean.
            if (!prior) {
                XCanaryContext.clear();
            }
        }
    }
}
