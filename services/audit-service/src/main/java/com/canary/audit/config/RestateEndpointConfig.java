package com.canary.audit.config;

import com.canary.audit.handler.AuditQueryServiceImpl;
import com.canary.audit.store.AuditEventStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.restate.sdk.endpoint.Endpoint;
import dev.restate.sdk.http.vertx.RestateHttpServer;
import io.vertx.core.http.HttpServer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration
@ConditionalOnProperty(
    name = "app.restate.register-handlers",
    havingValue = "true",
    matchIfMissing = true
)
public class RestateEndpointConfig {

    private final int port;
    private final AuditQueryServiceImpl handler;
    private HttpServer server;

    public RestateEndpointConfig(@Value("${app.restate.handler.port}") int port,
                                 AuditQueryServiceImpl handler) {
        this.port = port;
        this.handler = handler;
    }

    @Bean
    public static AuditQueryServiceImpl auditQueryServiceImpl(
            AuditEventStore store,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper
    ) {
        return new AuditQueryServiceImpl(store, kafkaTemplate, objectMapper);
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
