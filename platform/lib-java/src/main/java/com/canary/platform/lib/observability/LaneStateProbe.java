package com.canary.platform.lib.observability;

import io.fabric8.kubernetes.api.model.Endpoints;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class LaneStateProbe implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(LaneStateProbe.class);

    private final KubernetesClient client;
    private final MeterRegistry registry;
    private final String namespace;
    private final String serviceName;

    private final Map<String, AtomicInteger> laneState = new ConcurrentHashMap<>();
    private Watch stableWatch;
    private Watch canaryWatch;

    public LaneStateProbe(KubernetesClient client, MeterRegistry registry,
                          String namespace, String serviceName) {
        this.client = client;
        this.registry = registry;
        this.namespace = namespace;
        this.serviceName = serviceName;
    }

    public void start() {
        registerGauge("stable");
        registerGauge("canary");
        stableWatch = watchEndpoints(serviceName + "-stable", "stable");
        canaryWatch = watchEndpoints(serviceName + "-canary", "canary");
    }

    /** Visible for testing: directly toggle a lane's active value. */
    public void setLaneActive(String lane, boolean active) {
        laneState.computeIfAbsent(lane, l -> new AtomicInteger(0)).set(active ? 1 : 0);
    }

    private void registerGauge(String lane) {
        AtomicInteger holder = laneState.computeIfAbsent(lane, l -> new AtomicInteger(0));
        Gauge.builder("canary_lane_active", holder, AtomicInteger::doubleValue)
                .tags("service", serviceName, "lane", lane)
                .strongReference(true)
                .register(registry);
    }

    private Watch watchEndpoints(String endpointsName, String lane) {
        Endpoints e = client.endpoints().inNamespace(namespace).withName(endpointsName).get();
        setLaneActive(lane, hasAddresses(e));

        return client.endpoints().inNamespace(namespace).withName(endpointsName)
                .watch(new Watcher<>() {
                    @Override
                    public void eventReceived(Action action, Endpoints resource) {
                        boolean active = (action != Action.DELETED) && hasAddresses(resource);
                        setLaneActive(lane, active);
                    }

                    @Override
                    public void onClose(WatcherException cause) {
                        if (cause != null) {
                            log.warn("Endpoints watch for {} closed with error", endpointsName, cause);
                        }
                    }
                });
    }

    static boolean hasAddresses(Endpoints e) {
        if (e == null || e.getSubsets() == null) return false;
        return e.getSubsets().stream()
                .anyMatch(s -> s.getAddresses() != null && !s.getAddresses().isEmpty());
    }

    @Override
    public void close() {
        if (stableWatch != null) stableWatch.close();
        if (canaryWatch != null) canaryWatch.close();
    }
}
