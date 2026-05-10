package com.canary.platform.lib;

import com.canary.platform.lib.observability.CanaryMetrics;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class XCanaryRequestFilterTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final CanaryMetrics metrics = new CanaryMetrics(registry, "payment");
    private final XCanaryRequestFilter filter = new XCanaryRequestFilter(metrics);

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

    @Test
    void doFilterRecordsHttpMetricForCanaryRequest() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        CanaryMetrics metrics = new CanaryMetrics(registry, "payment");
        XCanaryRequestFilter filter = new XCanaryRequestFilter(metrics);

        MockHttpServletRequest req = new MockHttpServletRequest("GET", "/healthz");
        req.addHeader(XCanaryConstants.HEADER_NAME, XCanaryConstants.TRUE_VALUE);
        MockHttpServletResponse resp = new MockHttpServletResponse();
        resp.setStatus(200);

        filter.doFilter(req, resp, (r, s) -> {});

        Counter c = registry.find("canary_request_total")
                .tags("substrate", "http", "service", "payment", "lane", "canary", "outcome", "success")
                .counter();
        assertThat(c).isNotNull();
        assertThat(c.count()).isEqualTo(1.0);
    }

    @Test
    void doFilterTagsClientErrorOn4xx() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        CanaryMetrics metrics = new CanaryMetrics(registry, "payment");
        XCanaryRequestFilter filter = new XCanaryRequestFilter(metrics);

        MockHttpServletRequest req = new MockHttpServletRequest("POST", "/pay");
        MockHttpServletResponse resp = new MockHttpServletResponse();
        filter.doFilter(req, resp, (r, s) -> ((MockHttpServletResponse) s).setStatus(404));

        Counter c = registry.find("canary_request_total").tag("outcome", "client_error").counter();
        assertThat(c).isNotNull();
        assertThat(c.count()).isEqualTo(1.0);
    }

    @Test
    void doFilterTagsServerErrorOn5xx() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        CanaryMetrics metrics = new CanaryMetrics(registry, "payment");
        XCanaryRequestFilter filter = new XCanaryRequestFilter(metrics);

        MockHttpServletRequest req = new MockHttpServletRequest("POST", "/pay");
        MockHttpServletResponse resp = new MockHttpServletResponse();
        filter.doFilter(req, resp, (r, s) -> ((MockHttpServletResponse) s).setStatus(503));

        Counter c = registry.find("canary_request_total").tag("outcome", "server_error").counter();
        assertThat(c).isNotNull();
        assertThat(c.count()).isEqualTo(1.0);
    }
}
