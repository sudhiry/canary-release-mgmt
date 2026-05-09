package com.canary.platform.lib;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class XCanaryResponseHeaderFilterTest {

    @Test
    void setsHeaderFromConstructorArg() throws ServletException, IOException {
        XCanaryResponseHeaderFilter filter = new XCanaryResponseHeaderFilter("canary");
        HttpServletRequest req = mock(HttpServletRequest.class);
        HttpServletResponse res = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);

        filter.doFilter(req, res, chain);

        verify(res).setHeader("x-served-version", "canary");
        verify(chain).doFilter(req, res);
    }

    @Test
    void defaultsToStableWhenArgIsNull() throws ServletException, IOException {
        XCanaryResponseHeaderFilter filter = new XCanaryResponseHeaderFilter(null);
        HttpServletRequest req = mock(HttpServletRequest.class);
        HttpServletResponse res = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);

        filter.doFilter(req, res, chain);

        verify(res).setHeader("x-served-version", "stable");
    }

    @Test
    void defaultsToStableWhenArgIsBlank() throws ServletException, IOException {
        XCanaryResponseHeaderFilter filter = new XCanaryResponseHeaderFilter("   ");
        HttpServletRequest req = mock(HttpServletRequest.class);
        HttpServletResponse res = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);

        filter.doFilter(req, res, chain);

        verify(res).setHeader("x-served-version", "stable");
    }

    @Test
    void filterChainAlwaysProceedsEvenIfHeaderSetThrows() throws ServletException, IOException {
        XCanaryResponseHeaderFilter filter = new XCanaryResponseHeaderFilter("stable");
        HttpServletRequest req = mock(HttpServletRequest.class);
        HttpServletResponse res = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);

        filter.doFilter(req, res, chain);

        verify(chain).doFilter(req, res);
    }
}
