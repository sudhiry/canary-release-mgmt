package com.canary.inventory.kafka;

import com.canary.platform.lib.XCanaryKafkaProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaProducerConfigTest {

    @Test
    void producerFactoryHasXCanaryInterceptorConfigured() {
        var config = new KafkaProducerConfig();
        ProducerFactory<String, String> factory = config.producerFactory("localhost:9092");

        var props = ((DefaultKafkaProducerFactory<String, String>) factory).getConfigurationProperties();

        assertThat(props.get(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG))
            .isEqualTo(XCanaryKafkaProducerInterceptor.class.getName());
    }
}
