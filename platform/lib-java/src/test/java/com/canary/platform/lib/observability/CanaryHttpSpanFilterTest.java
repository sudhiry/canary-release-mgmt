package com.canary.platform.lib.observability;

import com.canary.platform.lib.XCanaryContext;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.verify;

class CanaryHttpSpanFilterTest {

    @AfterEach
    void cleanContext() {
        XCanaryContext.clear();
    }

    @Test
    void canaryRequestSetsLaneAttributeOnActiveSpan() throws Exception {
        Span mockSpan = Mockito.mock(Span.class);
        Mockito.when(mockSpan.setAttribute(Mockito.anyString(), Mockito.anyString())).thenReturn(mockSpan);
        doCallRealMethod().when(mockSpan).storeInContext(any());

        try (Scope ignored = Context.current().with(mockSpan).makeCurrent()) {
            XCanaryContext.set(true);
            CanaryHttpSpanFilter filter = new CanaryHttpSpanFilter("payment");
            filter.doFilter(new MockHttpServletRequest("GET", "/x"),
                            new MockHttpServletResponse(), new MockFilterChain());
        }

        verify(mockSpan).setAttribute("canary.lane", "canary");
        verify(mockSpan).setAttribute("canary.service", "payment");
    }

    @Test
    void stableRequestSetsStableLane() throws Exception {
        Span mockSpan = Mockito.mock(Span.class);
        Mockito.when(mockSpan.setAttribute(Mockito.anyString(), Mockito.anyString())).thenReturn(mockSpan);
        doCallRealMethod().when(mockSpan).storeInContext(any());

        try (Scope ignored = Context.current().with(mockSpan).makeCurrent()) {
            XCanaryContext.set(false);
            CanaryHttpSpanFilter filter = new CanaryHttpSpanFilter("payment");
            filter.doFilter(new MockHttpServletRequest("GET", "/x"),
                            new MockHttpServletResponse(), new MockFilterChain());
        }

        verify(mockSpan).setAttribute("canary.lane", "stable");
    }
}
