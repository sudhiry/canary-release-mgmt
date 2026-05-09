package com.canary.payment.config;

import com.canary.payment.store.ChargeStore;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
    }
}
