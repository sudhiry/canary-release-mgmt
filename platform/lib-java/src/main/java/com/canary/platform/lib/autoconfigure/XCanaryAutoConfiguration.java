package com.canary.platform.lib.autoconfigure;

import com.canary.platform.lib.KafkaConsumerHealthIndicator;
import com.canary.platform.lib.XCanaryConsumeFilter;
import com.canary.platform.lib.XCanaryConsumerGroupIdResolver;
import com.canary.platform.lib.XCanaryKafkaProducerInterceptor;
import com.canary.platform.lib.XCanaryPresenceWatcher;
import com.canary.platform.lib.XCanaryRequestFilter;
import com.canary.platform.lib.XCanaryResponseHeaderFilter;
import com.canary.platform.lib.XCanaryRestClientInterceptor;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import com.canary.platform.lib.XServedChainResponseFilter;
import com.canary.platform.lib.XServedChainRestClientInterceptor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestClient;

import java.util.Optional;
import java.util.function.Consumer;

@AutoConfiguration
public class XCanaryAutoConfiguration {

    @Bean
    public XCanaryRequestFilter xCanaryRequestFilter() {
        return new XCanaryRequestFilter();
    }

    @Bean
    public XCanaryResponseHeaderFilter xCanaryResponseHeaderFilter(
            @Value("${canary.version:${VERSION:stable}}") String version) {
        return new XCanaryResponseHeaderFilter(version);
    }

    @Bean
    public XServedChainResponseFilter xServedChainResponseFilter(
            @Value("${canary.service-name:${SERVICE_NAME:unknown}}") String serviceName,
            @Value("${canary.version:${VERSION:stable}}") String version) {
        return new XServedChainResponseFilter(serviceName, version);
    }

    @Bean
    public XServedChainRestClientInterceptor xServedChainRestClientInterceptor() {
        return new XServedChainRestClientInterceptor();
    }

    @Bean
    public XCanaryRestClientInterceptor xCanaryRestClientInterceptor() {
        return new XCanaryRestClientInterceptor();
    }

    @Bean
    public Consumer<RestClient.Builder> xCanaryRestClientCustomizer(
            XCanaryRestClientInterceptor canaryInterceptor,
            XServedChainRestClientInterceptor chainInterceptor) {
        return builder -> builder
                .requestInterceptor(canaryInterceptor)
                .requestInterceptor(chainInterceptor);
    }

    @Bean
    public XCanaryKafkaProducerInterceptor<Object, Object> xCanaryKafkaProducerInterceptor() {
        return new XCanaryKafkaProducerInterceptor<>();
    }

    @Bean
    public XCanaryRestateClientCustomizer xCanaryRestateClientCustomizer() {
        return new XCanaryRestateClientCustomizer();
    }

    // --- Phase 2.a additions ---

    @Bean
    public XCanaryConsumerGroupIdResolver xCanaryConsumerGroupIdResolver(
            @Value("${canary.version:${VERSION:stable}}") String version) {
        return new XCanaryConsumerGroupIdResolver(version);
    }

    @Bean
    public KafkaConsumerHealthIndicator kafkaConsumerHealthIndicator(
            @Value("${canary.kafka-health-timeout-ms:30000}") long timeoutMs) {
        return new KafkaConsumerHealthIndicator(timeoutMs);
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(name = "canary.presence-watcher.enabled", havingValue = "true", matchIfMissing = true)
    public XCanaryPresenceWatcher xCanaryPresenceWatcher(
            @Value("${canary.namespace:${POD_NAMESPACE:services}}") String namespace,
            @Value("${canary.service-name:${SERVICE_NAME:unknown}}") String serviceName) {
        XCanaryPresenceWatcher w = new XCanaryPresenceWatcher(namespace, serviceName);
        w.start();
        return w;
    }

    @Bean
    public XCanaryConsumeFilter xCanaryConsumeFilter(
            @Value("${canary.version:${VERSION:stable}}") String version,
            Optional<XCanaryPresenceWatcher> presenceWatcher) {
        return new XCanaryConsumeFilter(version,
                () -> presenceWatcher.map(XCanaryPresenceWatcher::isCanaryReady).orElse(false));
    }
}
