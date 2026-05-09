package com.canary.payment.config;

import com.canary.payment.handler.PaymentVOImpl;
import com.canary.payment.store.ChargeStore;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.restate.sdk.endpoint.Endpoint;
import org.springframework.kafka.core.KafkaTemplate;
import dev.restate.sdk.http.vertx.RestateHttpServer;
import io.vertx.core.http.HttpServer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Starts the Restate HTTP endpoint for the PaymentVO virtual object handler.
 * Gated by {@code app.restate.register-handlers} (defaults to {@code true}).
 * Set to {@code false} in tests and canary-only deployments where this node
 * should not register handlers.
 */
@Configuration
@ConditionalOnProperty(
    name = "app.restate.register-handlers",
    havingValue = "true",
    matchIfMissing = true
)
public class RestateEndpointConfig {

    private final int port;
    private final PaymentVOImpl handler;
    private HttpServer server;

    public RestateEndpointConfig(@Value("${app.restate.handler.port}") int port,
                                 PaymentVOImpl handler) {
        this.port = port;
        this.handler = handler;
    }

    @Bean
    public static PaymentVOImpl paymentVOImpl(
            ChargeStore store,
            XCanaryRestateClientCustomizer canary,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper
    ) {
        return new PaymentVOImpl(store, canary, kafkaTemplate, objectMapper);
    }

    @PostConstruct
    void start() throws Exception {
        Endpoint endpoint = Endpoint.builder().bind(handler).build();
        server = RestateHttpServer.fromEndpoint(endpoint);
        server.listen(port).toCompletionStage().toCompletableFuture().get();
    }

    @PreDestroy
    void stop() throws Exception {
        if (server != null) {
            server.close().toCompletionStage().toCompletableFuture().get();
        }
    }
}
