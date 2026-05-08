package com.canary.platform.lib;

import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;

import java.io.IOException;

public class XCanaryRestClientInterceptor implements ClientHttpRequestInterceptor {

    @Override
    public ClientHttpResponse intercept(HttpRequest request, byte[] body,
                                        ClientHttpRequestExecution execution) throws IOException {
        if (XCanaryContext.isCanary() && !request.getHeaders().containsHeader(XCanaryConstants.HEADER_NAME)) {
            request.getHeaders().add(XCanaryConstants.HEADER_NAME, XCanaryConstants.TRUE_VALUE);
        }
        return execution.execute(request, body);
    }
}
