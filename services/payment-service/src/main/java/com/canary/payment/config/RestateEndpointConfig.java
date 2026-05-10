package com.canary.payment.config;

import com.canary.payment.handler.PaymentVOImplCanary;
import com.canary.payment.handler.PaymentVOImplStable;
import com.canary.payment.store.ChargeStore;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.restate.sdk.endpoint.Endpoint;
import dev.restate.sdk.http.vertx.RestateHttpServer;
import io.vertx.core.http.HttpServer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Starts the Restate HTTP endpoint. Variant is determined by {@code app.version}:
 * "stable" wires {@link PaymentVOImplStable}; "canary" wires
 * {@link PaymentVOImplCanary}. Gated overall by
 * {@code app.restate.register-handlers} (default true).
 */
@Configuration
@ConditionalOnProperty(
    name = "app.restate.register-handlers",
    havingValue = "true",
    matchIfMissing = true
)
public class RestateEndpointConfig {

    private final int port;
    private final Object handler;   // Object so either Stable or Canary impl works
    private HttpServer server;

    public RestateEndpointConfig(@Value("${app.restate.handler.port}") int port,
                                 @Autowired(required = false)
                                     PaymentVOImplStable stableHandler,
                                 @Autowired(required = false)
                                     PaymentVOImplCanary canaryHandler) {
        this.port = port;
        if (stableHandler != null && canaryHandler != null) {
            throw new IllegalStateException(
                "Both stable and canary impls present; check app.version configuration");
        }
        if (stableHandler != null) this.handler = stableHandler;
        else if (canaryHandler != null) this.handler = canaryHandler;
        else throw new IllegalStateException(
            "No payment handler bean present; expected app.version=stable|canary");
    }

    @Bean
    @ConditionalOnProperty(name = "app.version", havingValue = "stable", matchIfMissing = true)
    public static PaymentVOImplStable paymentVOImplStable(
            ChargeStore store,
            XCanaryRestateClientCustomizer canary,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper) {
        return new PaymentVOImplStable(store, canary, kafkaTemplate, objectMapper);
    }

    @Bean
    @ConditionalOnProperty(name = "app.version", havingValue = "canary")
    public static PaymentVOImplCanary paymentVOImplCanary(
            ChargeStore store,
            XCanaryRestateClientCustomizer canary,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper) {
        return new PaymentVOImplCanary(store, canary, kafkaTemplate, objectMapper);
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
