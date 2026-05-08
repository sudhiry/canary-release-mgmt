package com.canary.platform.lib.autoconfigure;

import com.canary.platform.lib.XCanaryKafkaProducerInterceptor;
import com.canary.platform.lib.XCanaryRequestFilter;
import com.canary.platform.lib.XCanaryRestClientInterceptor;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Bean;

@AutoConfiguration
public class XCanaryAutoConfiguration {

    @Bean
    public XCanaryRequestFilter xCanaryRequestFilter() {
        return new XCanaryRequestFilter();
    }

    @Bean
    public XCanaryRestClientInterceptor xCanaryRestClientInterceptor() {
        return new XCanaryRestClientInterceptor();
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
