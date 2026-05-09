package com.canary.platform.lib.autoconfigure;

import com.canary.platform.lib.XCanaryKafkaProducerInterceptor;
import com.canary.platform.lib.XCanaryRequestFilter;
import com.canary.platform.lib.XCanaryResponseHeaderFilter;
import com.canary.platform.lib.XCanaryRestClientInterceptor;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import com.canary.platform.lib.XServedChainResponseFilter;
import com.canary.platform.lib.XServedChainRestClientInterceptor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestClient;

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
}
