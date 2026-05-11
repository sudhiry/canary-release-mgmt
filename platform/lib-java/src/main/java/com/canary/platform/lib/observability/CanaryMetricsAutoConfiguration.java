package com.canary.platform.lib.observability;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
@ConditionalOnClass(MeterRegistry.class)
public class CanaryMetricsAutoConfiguration {

    @Bean
    public CanaryMetrics canaryMetrics(
            MeterRegistry registry,
            @Value("${canary.service-name:${SERVICE_NAME:unknown}}") String serviceName) {
        return new CanaryMetrics(registry, serviceName);
    }

    @Bean
    public CanaryKafkaRecordInterceptor<Object, Object> canaryKafkaRecordInterceptor(CanaryMetrics metrics) {
        return new CanaryKafkaRecordInterceptor<>(metrics);
    }

    @Bean
    public CanaryRestateMeter canaryRestateMeter(CanaryMetrics metrics) {
        return new CanaryRestateMeter(metrics);
    }

    @Bean
    public CanaryHttpSpanFilter canaryHttpSpanFilter(
            @Value("${canary.service-name:${SERVICE_NAME:unknown}}") String serviceName) {
        return new CanaryHttpSpanFilter(serviceName);
    }

    @Bean
    @ConditionalOnMissingBean
    public KubernetesClient kubernetesClient() {
        return new KubernetesClientBuilder().build();
    }

    @Bean(destroyMethod = "close")
    public LaneStateProbe laneStateProbe(
            KubernetesClient client,
            MeterRegistry registry,
            @Value("${canary.namespace:${POD_NAMESPACE:services}}") String namespace,
            @Value("${canary.service-name:${SERVICE_NAME:unknown}}") String serviceName) {
        LaneStateProbe probe = new LaneStateProbe(client, registry, namespace, serviceName);
        probe.start();
        return probe;
    }
}
