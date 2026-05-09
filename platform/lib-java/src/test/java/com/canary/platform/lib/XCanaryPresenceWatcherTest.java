package com.canary.platform.lib;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodCondition;
import io.fabric8.kubernetes.api.model.PodConditionBuilder;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.PodStatusBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class XCanaryPresenceWatcherTest {

    private static Pod podWithReady(String name, boolean ready) {
        PodCondition cond = new PodConditionBuilder()
                .withType("Ready")
                .withStatus(ready ? "True" : "False")
                .build();
        PodStatus status = new PodStatusBuilder().withConditions(cond).build();
        return new PodBuilder()
                .withMetadata(new ObjectMetaBuilder().withName(name).build())
                .withStatus(status)
                .build();
    }

    @Test
    void computeCanaryReady_emptyListIsFalse() {
        assertFalse(XCanaryPresenceWatcher.computeCanaryReady(List.of()));
    }

    @Test
    void computeCanaryReady_singleReadyPodIsTrue() {
        assertTrue(XCanaryPresenceWatcher.computeCanaryReady(List.of(podWithReady("p1", true))));
    }

    @Test
    void computeCanaryReady_singleNotReadyPodIsFalse() {
        assertFalse(XCanaryPresenceWatcher.computeCanaryReady(List.of(podWithReady("p1", false))));
    }

    @Test
    void computeCanaryReady_anyReadyPodIsTrue() {
        assertTrue(XCanaryPresenceWatcher.computeCanaryReady(List.of(
                podWithReady("p1", false),
                podWithReady("p2", true),
                podWithReady("p3", false))));
    }

    @Test
    void computeCanaryReady_allNotReadyIsFalse() {
        assertFalse(XCanaryPresenceWatcher.computeCanaryReady(List.of(
                podWithReady("p1", false),
                podWithReady("p2", false))));
    }

    @Test
    void isPodReady_handlesNullStatus() {
        Pod p = new PodBuilder().withMetadata(new ObjectMetaBuilder().withName("p").build()).build();
        assertFalse(XCanaryPresenceWatcher.isPodReady(p));
    }

    @Test
    void isPodReady_handlesNullConditions() {
        Pod p = new PodBuilder()
                .withMetadata(new ObjectMetaBuilder().withName("p").build())
                .withStatus(new PodStatusBuilder().build())
                .build();
        assertFalse(XCanaryPresenceWatcher.isPodReady(p));
    }
}
