package com.canary.platform.lib;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class XServedChainResponseFilterTest {

    @AfterEach
    void clear() { XServedChainContext.clear(); }

    @Test
    void emitsOwnTokenOnlyWhenNoDownstreams() throws ServletException, IOException {
        XServedChainResponseFilter f = new XServedChainResponseFilter("payment-service", "stable");
        HttpServletRequest req = mock(HttpServletRequest.class);
        HttpServletResponse res = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);

        f.doFilter(req, res, chain);

        verify(res).setHeader("x-served-chain", "payment-service=stable");
        verify(chain).doFilter(req, res);
    }

    @Test
    void prependsOwnTokenToDownstreamChain() throws ServletException, IOException {
        XServedChainResponseFilter f = new XServedChainResponseFilter("order-service", "canary");
        HttpServletRequest req = mock(HttpServletRequest.class);
        HttpServletResponse res = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);

        // Simulate downstream calls populating the context during filterChain.doFilter().
        org.mockito.Mockito.doAnswer(inv -> {
            XServedChainContext.append("inventory-service=canary");
            XServedChainContext.append("audit-service=stable");
            return null;
        }).when(chain).doFilter(req, res);

        f.doFilter(req, res, chain);

        verify(res).setHeader("x-served-chain",
                "order-service=canary,inventory-service=canary,audit-service=stable");
    }

    @Test
    void clearsContextEvenIfChainThrows() {
        XServedChainResponseFilter f = new XServedChainResponseFilter("order-service", "canary");
        HttpServletRequest req = mock(HttpServletRequest.class);
        HttpServletResponse res = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);

        try {
            org.mockito.Mockito.doThrow(new RuntimeException("boom")).when(chain).doFilter(req, res);
            try { f.doFilter(req, res, chain); } catch (Exception ignored) {}
        } catch (Exception ignored) {}

        // After filter exits, context should be cleared.
        org.junit.jupiter.api.Assertions.assertTrue(XServedChainContext.tokens().isEmpty());
    }

    @Test
    void defaultsForNullArgs() throws ServletException, IOException {
        XServedChainResponseFilter f = new XServedChainResponseFilter(null, null);
        HttpServletRequest req = mock(HttpServletRequest.class);
        HttpServletResponse res = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);

        f.doFilter(req, res, chain);

        verify(res).setHeader("x-served-chain", "unknown=stable");
    }
}
