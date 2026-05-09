package com.canary.inventory.config;

import com.canary.inventory.handler.ReservationWorkflowImpl;
import com.canary.inventory.store.ReservationStore;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import dev.restate.sdk.endpoint.Endpoint;
import dev.restate.sdk.http.vertx.RestateHttpServer;
import io.vertx.core.http.HttpServer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Starts the Restate HTTP endpoint for the ReservationWorkflow handler.
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
    private final ReservationWorkflowImpl handler;
    private HttpServer server;

    public RestateEndpointConfig(@Value("${app.restate.handler.port}") int port,
                                 ReservationWorkflowImpl handler) {
        this.port = port;
        this.handler = handler;
    }

    @Bean
    public static ReservationWorkflowImpl reservationWorkflowImpl(
            ReservationStore store,
            XCanaryRestateClientCustomizer canary
    ) {
        return new ReservationWorkflowImpl(store, canary);
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
