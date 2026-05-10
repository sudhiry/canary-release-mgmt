package com.canary.platform.lib.observability;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

class TracingAutoConfigurationTest {

    @Test
    void registersJvmLaneStripperMeterFilter() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(TracingAutoConfiguration.class))
                .withUserConfiguration(TestSupport.class)
                .run(ctx -> {
                    assertThat(ctx).getBeans(MeterFilter.class).isNotEmpty();
                });
    }

    @Configuration
    static class TestSupport {
        @Bean MeterRegistry meterRegistry() { return new SimpleMeterRegistry(); }
    }
}
