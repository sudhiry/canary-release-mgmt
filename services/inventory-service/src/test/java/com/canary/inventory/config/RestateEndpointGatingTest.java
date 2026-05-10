package com.canary.inventory.config;

import com.canary.inventory.handler.ReservationWorkflowImplCanary;
import com.canary.inventory.handler.ReservationWorkflowImplStable;
import com.canary.inventory.store.ReservationStore;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

import static org.assertj.core.api.Assertions.assertThat;

class RestateEndpointGatingTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withUserConfiguration(TestStubs.class, RestateEndpointConfig.class)
        .withPropertyValues("app.restate.handler.port=0");

    @Test
    void whenFlagTrueThenEndpointConfigIsActive() {
        runner.withPropertyValues("app.restate.register-handlers=true")
            .run(ctx -> assertThat(ctx).hasSingleBean(RestateEndpointConfig.class));
    }

    @Test
    void whenFlagFalseThenEndpointConfigIsSkipped() {
        runner.withPropertyValues("app.restate.register-handlers=false")
            .run(ctx -> assertThat(ctx).doesNotHaveBean(RestateEndpointConfig.class));
    }

    @Test
    void whenFlagAbsentThenDefaultsToActive() {
        runner.run(ctx -> assertThat(ctx).hasSingleBean(RestateEndpointConfig.class));
    }

    @Test
    void wiresStableImplWhenVersionIsStable() {
        runner.withPropertyValues("app.version=stable", "app.restate.register-handlers=true")
            .run(ctx -> {
                assertThat(ctx).hasSingleBean(ReservationWorkflowImplStable.class);
                assertThat(ctx).doesNotHaveBean(ReservationWorkflowImplCanary.class);
            });
    }

    @Test
    void wiresCanaryImplWhenVersionIsCanary() {
        runner.withPropertyValues("app.version=canary", "app.restate.register-handlers=true")
            .run(ctx -> {
                assertThat(ctx).hasSingleBean(ReservationWorkflowImplCanary.class);
                assertThat(ctx).doesNotHaveBean(ReservationWorkflowImplStable.class);
            });
    }

    @Test
    void rejectsUnknownVersion() {
        runner.withPropertyValues("app.version=banana", "app.restate.register-handlers=true")
            .run(ctx -> assertThat(ctx).hasFailed());
    }

    @Configuration
    static class TestStubs {
        @Bean
        ReservationStore reservationStore() {
            return new ReservationStore();
        }

        @Bean
        XCanaryRestateClientCustomizer canary() {
            return new XCanaryRestateClientCustomizer();
        }

        @Bean
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafkaTemplate() {
            return Mockito.mock(KafkaTemplate.class);
        }

        @Bean
        ObjectMapper objectMapper() {
            return new ObjectMapper();
        }
    }
}
