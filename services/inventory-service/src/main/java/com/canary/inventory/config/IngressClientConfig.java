package com.canary.inventory.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

import java.util.List;
import java.util.function.Consumer;

@Configuration
public class IngressClientConfig {

    /**
     * Creates a RestClient pointed at the Restate ingress URL.
     *
     * lib-java's XCanaryAutoConfiguration registers a {@code Consumer<RestClient.Builder>}
     * that attaches XCanaryRestClientInterceptor. We apply all such customizers here so
     * the ingress client automatically stamps {@code x-canary} on outbound calls when
     * the request thread is in canary context.
     */
    @Bean
    public RestClient ingressRestClient(
            @Value("${app.restate.ingress.url}") String ingressUrl,
            List<Consumer<RestClient.Builder>> builderCustomizers
    ) {
        RestClient.Builder builder = RestClient.builder().baseUrl(ingressUrl);
        builderCustomizers.forEach(c -> c.accept(builder));
        return builder.build();
    }
}
