package com.canary.platform.lib;

import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;

import java.io.IOException;
import java.util.List;

public class XServedChainRestClientInterceptor implements ClientHttpRequestInterceptor {

    @Override
    public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution)
            throws IOException {
        ClientHttpResponse response = execution.execute(request, body);
        List<String> headers = response.getHeaders().get(XServedChainContext.HEADER_NAME);
        if (headers != null && !headers.isEmpty()) {
            for (String h : headers) {
                XServedChainContext.appendChain(h);
            }
        }
        return response;
    }
}
