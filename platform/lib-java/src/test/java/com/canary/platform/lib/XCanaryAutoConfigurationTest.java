package com.canary.platform.lib;

import com.canary.platform.lib.autoconfigure.XCanaryAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

class XCanaryAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(XCanaryAutoConfiguration.class));

    @Test
    void registersFilterAndInterceptorBeans() {
        runner.run(ctx -> {
            assertThat(ctx).hasSingleBean(XCanaryRequestFilter.class);
            assertThat(ctx).hasSingleBean(XCanaryRestClientInterceptor.class);
            assertThat(ctx).hasSingleBean(XCanaryKafkaProducerInterceptor.class);
            assertThat(ctx).hasSingleBean(XCanaryRestateClientCustomizer.class);
        });
    }
}
