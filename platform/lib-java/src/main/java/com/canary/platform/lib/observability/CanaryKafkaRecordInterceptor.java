package com.canary.platform.lib.observability;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.RecordInterceptor;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CanaryKafkaRecordInterceptor<K, V> implements RecordInterceptor<K, V> {

    private final CanaryMetrics metrics;
    // start-time per record, identified by topic+partition+offset (handler chains may interleave)
    private final ConcurrentMap<String, Long> starts = new ConcurrentHashMap<>();

    public CanaryKafkaRecordInterceptor(CanaryMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    public ConsumerRecord<K, V> intercept(ConsumerRecord<K, V> record, Consumer<K, V> consumer) {
        starts.put(key(record), System.nanoTime());
        return record;
    }

    @Override
    public void success(ConsumerRecord<K, V> record, Consumer<K, V> consumer) {
        record(record, "success");
    }

    @Override
    public void failure(ConsumerRecord<K, V> record, Exception exception, Consumer<K, V> consumer) {
        record(record, "server_error");
    }

    private void record(ConsumerRecord<K, V> r, String outcome) {
        Long start = starts.remove(key(r));
        Duration elapsed = (start == null)
                ? Duration.ZERO
                : Duration.ofNanos(System.nanoTime() - start);
        metrics.recordKafka(r.topic(), outcome, elapsed);
    }

    private static String key(ConsumerRecord<?, ?> r) {
        return r.topic() + ":" + r.partition() + ":" + r.offset();
    }
}
