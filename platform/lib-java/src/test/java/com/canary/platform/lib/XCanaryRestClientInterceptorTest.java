package com.canary.platform.lib;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpResponse;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class XCanaryRestClientInterceptorTest {

    private final XCanaryRestClientInterceptor interceptor = new XCanaryRestClientInterceptor();

    @AfterEach
    void clearContext() {
        XCanaryContext.clear();
    }

    @Test
    void addsHeaderWhenContextIsCanary() throws IOException {
        XCanaryContext.set(true);
        HttpRequest request = mock(HttpRequest.class);
        HttpHeaders headers = new HttpHeaders();
        when(request.getHeaders()).thenReturn(headers);
        ClientHttpRequestExecution execution = mock(ClientHttpRequestExecution.class);
        ClientHttpResponse response = mock(ClientHttpResponse.class);
        when(execution.execute(any(), any())).thenReturn(response);

        interceptor.intercept(request, new byte[0], execution);

        assertThat(headers.getFirst("x-canary")).isEqualTo("true");
    }

    @Test
    void doesNotAddHeaderWhenContextIsFalse() throws IOException {
        XCanaryContext.set(false);
        HttpRequest request = mock(HttpRequest.class);
        HttpHeaders headers = new HttpHeaders();
        when(request.getHeaders()).thenReturn(headers);
        ClientHttpRequestExecution execution = mock(ClientHttpRequestExecution.class);
        ClientHttpResponse response = mock(ClientHttpResponse.class);
        when(execution.execute(any(), any())).thenReturn(response);

        interceptor.intercept(request, new byte[0], execution);

        assertThat(headers.containsHeader("x-canary")).isFalse();
    }

    @Test
    void doesNotOverwriteExistingHeader() throws IOException {
        XCanaryContext.set(true);
        HttpRequest request = mock(HttpRequest.class);
        HttpHeaders headers = new HttpHeaders();
        headers.add("x-canary", "preset-by-caller");
        when(request.getHeaders()).thenReturn(headers);
        ClientHttpRequestExecution execution = mock(ClientHttpRequestExecution.class);
        ClientHttpResponse response = mock(ClientHttpResponse.class);
        when(execution.execute(any(), any())).thenReturn(response);

        interceptor.intercept(request, new byte[0], execution);

        assertThat(headers.get("x-canary")).containsExactly("preset-by-caller");
    }
}
