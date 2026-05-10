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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.client.RestClient;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

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
            @Value("${canary.kafka-heartbeat-stale-ms:${canary.kafka-health-timeout-ms:15000}}") long heartbeatStaleMs,
            Supplier<OptionalLong> lastHeartbeatAgeMsSupplier) {
        return new KafkaConsumerHealthIndicator(heartbeatStaleMs, lastHeartbeatAgeMsSupplier);
    }

    @Bean
    public Supplier<OptionalLong> lastHeartbeatAgeMsSupplier(KafkaListenerEndpointRegistry registry) {
        return () -> {
            long minAgeMs = Long.MAX_VALUE;
            for (MessageListenerContainer container : registry.getListenerContainers()) {
                Map<String, Map<MetricName, ? extends Metric>> metrics = container.metrics();
                for (Map<MetricName, ? extends Metric> perClient : metrics.values()) {
                    for (Map.Entry<MetricName, ? extends Metric> entry : perClient.entrySet()) {
                        if ("last-heartbeat-seconds-ago".equals(entry.getKey().name())) {
                            Object value = entry.getValue().metricValue();
                            if (value instanceof Double d && !d.isNaN() && !d.isInfinite() && d >= 0) {
                                long ageMs = (long) (d * 1000);
                                if (ageMs < minAgeMs) minAgeMs = ageMs;
                            }
                        }
                    }
                }
            }
            return minAgeMs == Long.MAX_VALUE ? OptionalLong.empty() : OptionalLong.of(minAgeMs);
        };
    }

    // Spring Boot 4 / spring-kafka 4.0.4 dropped the
    // ConsumerPartitionsAssignedEvent / ConsumerPartitionsRevokedEvent
    // application events. To drive the indicator's lifecycle we set a
    // ConsumerAwareRebalanceListener on the listener container factory's
    // ContainerProperties; spring-kafka invokes it directly on the
    // consumer thread when the broker assigns or revokes partitions.
    // Override the 2-arg (consumer-aware) callbacks only — NOT the 1-arg
    // variants. The spring-kafka 4.0.4 dispatcher
    // (KafkaMessageListenerContainer$ListenerConsumer$ListenerConsumerRebalanceListener)
    // invokes BOTH the consumer-aware (2-arg) variant AND the user-listener
    // (1-arg) variant for onPartitionsAssigned and onPartitionsLost when the
    // same instance is referenced by both fields, because the 2-arg default
    // delegates to the 1-arg. Overriding only the 2-arg variants here keeps
    // the indicator callback firing exactly once per rebalance.
    @Bean
    public ConsumerAwareRebalanceListener kafkaConsumerRebalanceListener(
            KafkaConsumerHealthIndicator indicator) {
        return new ConsumerAwareRebalanceListener() {
            @Override
            public void onPartitionsAssigned(org.apache.kafka.clients.consumer.Consumer<?, ?> consumer,
                                             Collection<TopicPartition> partitions) {
                // Filter empty assigns: the broker can fire onPartitionsAssigned with
                // an empty collection during initial join; flipping the indicator to
                // assigned=true at that point would falsely report Ready before any
                // partitions actually belong to this consumer.
                if (!partitions.isEmpty()) {
                    indicator.onPartitionsAssigned();
                }
            }

            @Override
            public void onPartitionsRevokedBeforeCommit(org.apache.kafka.clients.consumer.Consumer<?, ?> consumer,
                                                       Collection<TopicPartition> partitions) {
                indicator.onPartitionsRevoked();
            }

            @Override
            public void onPartitionsLost(org.apache.kafka.clients.consumer.Consumer<?, ?> consumer,
                                         Collection<TopicPartition> partitions) {
                // Lost = broker fenced us (missed heartbeats). Same end state as revoked
                // for the indicator; treat as unassigned. (See I2 review note: distinct
                // diagnostic state for "lost" vs "revoked" is a future enhancement.)
                indicator.onPartitionsRevoked();
            }
        };
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

    // Spring Boot 4.0.4's KafkaAutoConfiguration no longer auto-creates
    // either ConsumerFactory or kafkaListenerContainerFactory (regression
    // from 3.x). Without both, @KafkaListener + @EnableKafka in services
    // fails at startup with "No qualifying bean of type ConsumerFactory" /
    // "No bean named 'kafkaListenerContainerFactory'". Surfaced during
    // cluster verification of Phase 2.b — kafka-consumer-groups.sh --list
    // showed zero Java consumer groups.
    //
    // Build both beans here. `auto.offset.reset = earliest` is preserved
    // from Phase 2.b: a brand-new <svc>-canary consumer group joining at
    // the LATEST offset by default would miss any messages produced before
    // it joined — relevant for replaying pre-existing topic data, and for
    // pre-warm flows that seed offsets before the canary subscribes.
    // (Cold-cluster readiness no longer depends on this; heartbeat-based
    // gating in KafkaConsumerHealthIndicator handles it directly.)

    @Bean
    @ConditionalOnMissingBean(ConsumerFactory.class)
    public ConsumerFactory<Object, Object> kafkaConsumerFactory(
            @Value("${spring.kafka.bootstrap-servers:${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}}") String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    @ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory,
            ConsumerAwareRebalanceListener rebalanceListener) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setConsumerRebalanceListener(rebalanceListener);
        return factory;
    }
}
