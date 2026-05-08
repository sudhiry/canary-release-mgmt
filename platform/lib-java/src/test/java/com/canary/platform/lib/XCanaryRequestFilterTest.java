package com.canary.platform.lib;

import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class XCanaryRequestFilterTest {

    private final XCanaryRequestFilter filter = new XCanaryRequestFilter();

    @AfterEach
    void clearContext() {
        XCanaryContext.clear();
    }

    @Test
    void setsContextWhenHeaderTrue() throws Exception {
        HttpServletRequest req = mock(HttpServletRequest.class);
        HttpServletResponse res = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);
        when(req.getHeader("x-canary")).thenReturn("true");

        boolean[] capturedDuringDoFilter = { false };
        doAnswer(inv -> {
            capturedDuringDoFilter[0] = XCanaryContext.isCanary();
            return null;
        }).when(chain).doFilter(req, res);

        filter.doFilter(req, res, chain);

        assertThat(capturedDuringDoFilter[0]).isTrue();
        // Always cleared after request completes.
        assertThat(XCanaryContext.isCanary()).isFalse();
    }

    @Test
    void leavesContextFalseWhenHeaderAbsent() throws Exception {
        HttpServletRequest req = mock(HttpServletRequest.class);
        HttpServletResponse res = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);
        when(req.getHeader("x-canary")).thenReturn(null);

        filter.doFilter(req, res, chain);

        assertThat(XCanaryContext.isCanary()).isFalse();
    }

    @Test
    void leavesContextFalseWhenHeaderIsAnythingButTrue() throws Exception {
        HttpServletRequest req = mock(HttpServletRequest.class);
        HttpServletResponse res = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);
        when(req.getHeader("x-canary")).thenReturn("yes");

        filter.doFilter(req, res, chain);

        assertThat(XCanaryContext.isCanary()).isFalse();
    }

    @Test
    void clearsContextEvenIfChainThrows() throws Exception {
        HttpServletRequest req = mock(HttpServletRequest.class);
        HttpServletResponse res = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);
        when(req.getHeader("x-canary")).thenReturn("true");
        doThrow(new RuntimeException("boom")).when(chain).doFilter(req, res);

        try {
            filter.doFilter(req, res, chain);
        } catch (RuntimeException ignored) {
        }

        assertThat(XCanaryContext.isCanary()).isFalse();
    }
}
