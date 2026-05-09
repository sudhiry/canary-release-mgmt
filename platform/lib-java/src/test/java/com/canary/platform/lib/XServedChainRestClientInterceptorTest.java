package com.canary.platform.lib;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpResponse;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class XServedChainRestClientInterceptorTest {
    @AfterEach
    void clear() { XServedChainContext.clear(); }

    @Test
    void capturesDownstreamChainHeader() throws IOException {
        XServedChainRestClientInterceptor it = new XServedChainRestClientInterceptor();
        HttpRequest req = mock(HttpRequest.class);
        ClientHttpResponse res = mock(ClientHttpResponse.class);
        HttpHeaders headers = new HttpHeaders();
        headers.add("x-served-chain", "inventory-service=canary,audit-service=stable");
        when(res.getHeaders()).thenReturn(headers);
        ClientHttpRequestExecution exec = mock(ClientHttpRequestExecution.class);
        when(exec.execute(any(), any())).thenReturn(res);

        it.intercept(req, new byte[0], exec);

        assertEquals(List.of("inventory-service=canary", "audit-service=stable"),
                     XServedChainContext.tokens());
    }

    @Test
    void noOpWhenHeaderAbsent() throws IOException {
        XServedChainRestClientInterceptor it = new XServedChainRestClientInterceptor();
        HttpRequest req = mock(HttpRequest.class);
        ClientHttpResponse res = mock(ClientHttpResponse.class);
        when(res.getHeaders()).thenReturn(new HttpHeaders());
        ClientHttpRequestExecution exec = mock(ClientHttpRequestExecution.class);
        when(exec.execute(any(), any())).thenReturn(res);

        it.intercept(req, new byte[0], exec);

        org.junit.jupiter.api.Assertions.assertTrue(XServedChainContext.tokens().isEmpty());
    }
}
