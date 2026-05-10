package com.canary.platform.lib.observability;

import io.fabric8.kubernetes.api.model.EndpointAddressBuilder;
import io.fabric8.kubernetes.api.model.EndpointSubsetBuilder;
import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.api.model.EndpointsBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LaneStateProbeTest {

    private SimpleMeterRegistry registry;
    private KubernetesClient client;

    @BeforeEach
    @SuppressWarnings({"unchecked", "rawtypes"})
    void setUp() {
        registry = new SimpleMeterRegistry();
        client = mock(KubernetesClient.class);
        // Stub the endpoints DSL chain so probe.start() does not NPE
        MixedOperation endpointsDsl = mock(MixedOperation.class);
        NonNamespaceOperation namespaceOp = mock(NonNamespaceOperation.class);
        Resource resource = mock(Resource.class);
        when(client.endpoints()).thenReturn(endpointsDsl);
        when(endpointsDsl.inNamespace(any())).thenReturn(namespaceOp);
        when(namespaceOp.withName(any())).thenReturn(resource);
        when(resource.get()).thenReturn(null);   // initial state: no endpoints
        when(resource.watch(any())).thenReturn(mock(io.fabric8.kubernetes.client.Watch.class));
    }

    @Test
    void startRegistersGaugesForBothLanes() {
        LaneStateProbe probe = new LaneStateProbe(client, registry, "services", "payment");
        probe.start();

        Gauge stable = registry.find("canary_lane_active")
                .tags("substrate", "http", "service", "payment", "lane", "stable").gauge();
        Gauge canary = registry.find("canary_lane_active")
                .tags("substrate", "http", "service", "payment", "lane", "canary").gauge();
        assertThat(stable).isNotNull();
        assertThat(canary).isNotNull();
        assertThat(stable.value()).isEqualTo(0.0);
        assertThat(canary.value()).isEqualTo(0.0);

        probe.close();
    }

    @Test
    void setLaneActiveTogglesGaugeValue() {
        LaneStateProbe probe = new LaneStateProbe(client, registry, "services", "payment");
        probe.start();

        probe.setLaneActive("canary", true);

        Gauge canary = registry.find("canary_lane_active")
                .tags("substrate", "http", "service", "payment", "lane", "canary").gauge();
        assertThat(canary.value()).isEqualTo(1.0);

        probe.setLaneActive("canary", false);
        assertThat(canary.value()).isEqualTo(0.0);

        probe.close();
    }

    @Test
    void hasAddressesReturnsTrueForPopulatedEndpoints() {
        Endpoints e = new EndpointsBuilder()
                .withNewMetadata().withName("svc").endMetadata()
                .addToSubsets(new EndpointSubsetBuilder()
                        .addToAddresses(new EndpointAddressBuilder().withIp("10.0.0.1").build())
                        .build())
                .build();
        assertThat(LaneStateProbe.hasAddresses(e)).isTrue();
    }

    @Test
    void hasAddressesReturnsFalseForNullOrEmpty() {
        assertThat(LaneStateProbe.hasAddresses(null)).isFalse();
        Endpoints e = new EndpointsBuilder().withNewMetadata().withName("svc").endMetadata().build();
        assertThat(LaneStateProbe.hasAddresses(e)).isFalse();
    }
}
