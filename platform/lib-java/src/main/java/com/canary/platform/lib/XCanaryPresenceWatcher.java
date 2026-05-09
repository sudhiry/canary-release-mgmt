package com.canary.platform.lib;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodCondition;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Watches canary pods (label selector app=&lt;svc&gt;,version=canary) in the
 * service's namespace. Maintains an in-memory canaryReady flag that flips
 * push-style as pods enter/leave Ready.
 *
 * Lifecycle: start() opens the watch and an initial list. close() shuts
 * the watch and the underlying KubernetesClient.
 */
public class XCanaryPresenceWatcher implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(XCanaryPresenceWatcher.class);

    private final String namespace;
    private final String serviceName;
    private final KubernetesClient client;
    private final AtomicBoolean canaryReady = new AtomicBoolean(false);
    private final Map<String, Boolean> podReadyByName = new HashMap<>();
    private volatile Watch watch;

    public XCanaryPresenceWatcher(String namespace, String serviceName) {
        this(namespace, serviceName, new KubernetesClientBuilder().build());
    }

    XCanaryPresenceWatcher(String namespace, String serviceName, KubernetesClient client) {
        this.namespace = namespace;
        this.serviceName = serviceName;
        this.client = client;
    }

    public void start() {
        // Initial list to populate state.
        List<Pod> initial = client.pods()
                .inNamespace(namespace)
                .withLabel("app", serviceName)
                .withLabel("version", "canary")
                .list()
                .getItems();
        synchronized (podReadyByName) {
            podReadyByName.clear();
            for (Pod p : initial) {
                podReadyByName.put(p.getMetadata().getName(), isPodReady(p));
            }
            canaryReady.set(podReadyByName.values().stream().anyMatch(Boolean::booleanValue));
        }
        LOG.info("XCanaryPresenceWatcher initial state: canaryReady={} pods={}",
                canaryReady.get(), podReadyByName.size());

        // Open long-lived watch.
        watch = client.pods()
                .inNamespace(namespace)
                .withLabel("app", serviceName)
                .withLabel("version", "canary")
                .watch(new PodWatcher());
    }

    public boolean isCanaryReady() {
        return canaryReady.get();
    }

    @Override
    public void close() {
        if (watch != null) {
            try { watch.close(); } catch (Exception ignored) {}
        }
        try { client.close(); } catch (Exception ignored) {}
    }

    private class PodWatcher implements Watcher<Pod> {
        @Override
        public void eventReceived(Action action, Pod pod) {
            String name = pod.getMetadata().getName();
            synchronized (podReadyByName) {
                if (action == Action.DELETED) {
                    podReadyByName.remove(name);
                } else {
                    podReadyByName.put(name, isPodReady(pod));
                }
                boolean ready = podReadyByName.values().stream().anyMatch(Boolean::booleanValue);
                boolean prior = canaryReady.getAndSet(ready);
                if (prior != ready) {
                    LOG.info("XCanaryPresenceWatcher canaryReady transition: {} -> {} (pod={})",
                            prior, ready, name);
                }
            }
        }

        @Override
        public void onClose(WatcherException cause) {
            LOG.warn("XCanaryPresenceWatcher watch closed; will rely on fabric8 auto-reconnect", cause);
        }
    }

    /** Pure function for unit testing. */
    static boolean computeCanaryReady(List<Pod> pods) {
        return pods.stream().anyMatch(XCanaryPresenceWatcher::isPodReady);
    }

    /** Pure function for unit testing. */
    static boolean isPodReady(Pod pod) {
        if (pod == null || pod.getStatus() == null || pod.getStatus().getConditions() == null) {
            return false;
        }
        for (PodCondition c : pod.getStatus().getConditions()) {
            if ("Ready".equals(c.getType()) && "True".equals(c.getStatus())) {
                return true;
            }
        }
        return false;
    }
}
