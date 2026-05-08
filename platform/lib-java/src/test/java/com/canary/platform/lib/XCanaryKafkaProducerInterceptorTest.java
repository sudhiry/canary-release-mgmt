package com.canary.platform.lib;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

class XCanaryKafkaProducerInterceptorTest {

    private final XCanaryKafkaProducerInterceptor<String, String> interceptor =
            new XCanaryKafkaProducerInterceptor<>();

    @AfterEach
    void clearContext() {
        XCanaryContext.clear();
    }

    @Test
    void addsHeaderWhenContextIsCanary() {
        interceptor.configure(Map.of());
        XCanaryContext.set(true);
        ProducerRecord<String, String> record = new ProducerRecord<>("topic", "k", "v");

        ProducerRecord<String, String> result = interceptor.onSend(record);

        assertThat(headerValue(result, "x-canary")).isEqualTo("true");
    }

    @Test
    void doesNotAddHeaderWhenContextIsFalse() {
        interceptor.configure(Map.of());
        XCanaryContext.set(false);
        ProducerRecord<String, String> record = new ProducerRecord<>("topic", "k", "v");

        ProducerRecord<String, String> result = interceptor.onSend(record);

        assertThat(StreamSupport.stream(result.headers().headers("x-canary").spliterator(), false))
                .isEmpty();
    }

    @Test
    void doesNotOverwriteCallerSetHeader() {
        interceptor.configure(Map.of());
        XCanaryContext.set(true);
        ProducerRecord<String, String> record = new ProducerRecord<>("topic", "k", "v");
        record.headers().add("x-canary", "preset".getBytes(StandardCharsets.UTF_8));

        ProducerRecord<String, String> result = interceptor.onSend(record);

        assertThat(headerValue(result, "x-canary")).isEqualTo("preset");
    }

    private static String headerValue(ProducerRecord<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }
}
