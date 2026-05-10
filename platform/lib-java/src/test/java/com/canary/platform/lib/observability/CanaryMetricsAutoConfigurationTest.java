package com.canary.platform.lib.observability;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CanaryMetricsAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(CanaryMetricsAutoConfiguration.class))
            .withUserConfiguration(TestSupport.class)
            .withPropertyValues("canary.service-name=payment");

    @Test
    void registersAllObservabilityBeans() {
        runner.run(ctx -> {
            assertThat(ctx).hasSingleBean(CanaryMetrics.class);
            assertThat(ctx).hasSingleBean(CanaryKafkaRecordInterceptor.class);
            assertThat(ctx).hasSingleBean(CanaryRestateMeter.class);
            assertThat(ctx).hasSingleBean(CanaryHttpSpanFilter.class);
            assertThat(ctx).hasSingleBean(LaneStateProbe.class);
        });
    }

    @Configuration
    static class TestSupport {
        @Bean MeterRegistry meterRegistry() { return new SimpleMeterRegistry(); }

        @Bean
        @SuppressWarnings({"unchecked", "rawtypes"})
        KubernetesClient kubernetesClient() {
            KubernetesClient client = mock(KubernetesClient.class);
            // Stub the endpoints DSL chain so LaneStateProbe.start() does not NPE
            MixedOperation endpointsDsl = mock(MixedOperation.class);
            NonNamespaceOperation namespaceOp = mock(NonNamespaceOperation.class);
            Resource resource = mock(Resource.class);
            when(client.endpoints()).thenReturn(endpointsDsl);
            when(endpointsDsl.inNamespace(any())).thenReturn(namespaceOp);
            when(namespaceOp.withName(any())).thenReturn(resource);
            when(resource.get()).thenReturn(null);   // initial state: no endpoints
            when(resource.watch(any())).thenReturn(mock(io.fabric8.kubernetes.client.Watch.class));
            return client;
        }
    }
}
