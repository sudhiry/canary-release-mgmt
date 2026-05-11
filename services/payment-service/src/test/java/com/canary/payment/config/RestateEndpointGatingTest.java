package com.canary.payment.config;

import com.canary.payment.handler.PaymentVOImplCanary;
import com.canary.payment.handler.PaymentVOImplStable;
import com.canary.payment.store.ChargeStore;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import com.canary.platform.lib.observability.CanaryMetrics;
import com.canary.platform.lib.observability.CanaryRestateMeter;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
                assertThat(ctx).hasSingleBean(PaymentVOImplStable.class);
                assertThat(ctx).doesNotHaveBean(PaymentVOImplCanary.class);
            });
    }

    @Test
    void wiresCanaryImplWhenVersionIsCanary() {
        runner.withPropertyValues("app.version=canary", "app.restate.register-handlers=true")
            .run(ctx -> {
                assertThat(ctx).hasSingleBean(PaymentVOImplCanary.class);
                assertThat(ctx).doesNotHaveBean(PaymentVOImplStable.class);
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
        ChargeStore chargeStore() {
            return new ChargeStore();
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

        @Bean
        CanaryRestateMeter canaryRestateMeter() {
            return new CanaryRestateMeter(new CanaryMetrics(new SimpleMeterRegistry(), "payment"));
        }
    }
}
