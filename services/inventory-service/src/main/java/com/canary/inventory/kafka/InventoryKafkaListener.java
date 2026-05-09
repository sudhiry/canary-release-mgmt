package com.canary.inventory.kafka;

import com.canary.inventory.store.ConsumedEvent;
import com.canary.inventory.store.ConsumedEventStore;
import com.canary.platform.lib.KafkaConsumerHealthIndicator;
import com.canary.platform.lib.XCanaryConsumeContext;
import com.canary.platform.lib.XCanaryConsumeFilter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Component
@ConditionalOnProperty(
    name = "app.kafka.consumers.enabled",
    havingValue = "true",
    matchIfMissing = true
)
public class InventoryKafkaListener {

    private final ConsumedEventStore store;
    private final XCanaryConsumeFilter filter;
    private final KafkaConsumerHealthIndicator health;

    public InventoryKafkaListener(ConsumedEventStore store,
                                  XCanaryConsumeFilter filter,
                                  KafkaConsumerHealthIndicator health) {
        this.store = store;
        this.filter = filter;
        this.health = health;
    }

    @KafkaListener(
        topics = "orders.events",
        groupId = "#{xCanaryConsumerGroupIdResolver.resolve('inventory-service')}"
    )
    public void onMessage(ConsumerRecord<String, String> record) {
        health.recordPoll();
        if (!filter.shouldProcess(record.headers())) {
            return;
        }
        XCanaryConsumeContext.runWith(record.headers(), () -> {
            Map<String, String> headers = new HashMap<>();
            record.headers().forEach(h -> headers.put(h.key(), new String(h.value(), StandardCharsets.UTF_8)));
            store.record(new ConsumedEvent(record.topic(), record.key(), record.value(), headers));
        });
    }
}
