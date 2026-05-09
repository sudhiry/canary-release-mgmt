# Phase 2.a — Kafka canary consumer foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the lib-java + lib-node foundation that lets each service join a per-subset Kafka consumer group with a per-message header filter; detect canary subset presence via a long-lived k8s Pod watch; gate canary readiness probe on Kafka consumer health; propagate `x-canary` from consume context to outbound calls. Plus the Helm chart RBAC needed for the watch.

**Architecture:** Two new lib modules per language. `XCanaryPresenceWatcher` opens a long-lived k8s Pod watch (label selector `app=<svc>,version=canary`) and maintains an in-memory `canaryReady` flag. `XCanaryConsumeFilter` is a pure-function decision used per Kafka message. `XCanaryConsumeContext` wraps the message handler in a `runWith` frame so outbound calls inherit `x-canary`. `KafkaConsumerHealthIndicator` (Spring) / `kafkaConsumerHealth` (Node) tracks last-successful-poll for the readiness probe. A new Helm Role + RoleBinding grants `pods` get/list/watch in the service's namespace.

**Tech Stack:**
- Spring Boot 4 + Java 25 (existing)
- Express + Node 25 (existing)
- `io.fabric8:kubernetes-client` 7.x (NEW dep on Java side)
- `@kubernetes/client-node` 1.x (NEW dep on Node side)
- vitest 2.x + JUnit 5 (existing test frameworks)

**Spec reference:** `docs/superpowers/specs/2026-05-10-canary-release-phase-2-a-kafka-canary-foundation-design.md`

---

## Prerequisites

- Phase 1 fully merged (Plans 1.1 through 1.5.b on main).
- `make verify` is green on main before this branch starts.
- Worktree on `claude/phase-2.a-kafka-canary-foundation` from main HEAD.

---

## File Structure

```
platform/lib-java/                                                      # MODIFY
├── build.gradle.kts                                                    # MODIFY: add fabric8 kubernetes-client
├── src/main/java/com/canary/platform/lib/
│   ├── XCanaryConsumerGroupIdResolver.java                             # NEW (Task 2)
│   ├── XCanaryConsumeFilter.java                                       # NEW (Task 3)
│   ├── XCanaryConsumeContext.java                                      # NEW (Task 4)
│   ├── XCanaryPresenceWatcher.java                                     # NEW (Task 5)
│   ├── KafkaConsumerHealthIndicator.java                               # NEW (Task 6)
│   └── autoconfigure/XCanaryAutoConfiguration.java                     # MODIFY (Task 7)
└── src/test/java/com/canary/platform/lib/                              # NEW tests
    ├── XCanaryConsumerGroupIdResolverTest.java                         (Task 2)
    ├── XCanaryConsumeFilterTest.java                                   (Task 3)
    ├── XCanaryConsumeContextTest.java                                  (Task 4)
    ├── XCanaryPresenceWatcherTest.java                                 (Task 5; pure-fn tests only)
    └── KafkaConsumerHealthIndicatorTest.java                           (Task 6)

platform/lib-node/                                                      # MODIFY
├── package.json                                                        # MODIFY: add @kubernetes/client-node
├── src/
│   ├── x-canary-consumer-group.ts                                      # NEW (Task 9)
│   ├── x-canary-consume-filter.ts                                      # NEW (Task 10)
│   ├── x-canary-consume-context.ts                                     # NEW (Task 11)
│   ├── x-canary-presence-watcher.ts                                    # NEW (Task 12)
│   ├── kafka-consumer-health.ts                                        # NEW (Task 13)
│   ├── index.ts                                                        # MODIFY (Task 14)
│   └── __tests__/                                                       # NEW tests
│       ├── x-canary-consumer-group.test.ts                             (Task 9)
│       ├── x-canary-consume-filter.test.ts                             (Task 10)
│       ├── x-canary-consume-context.test.ts                            (Task 11)
│       ├── x-canary-presence-watcher.test.ts                           (Task 12; pure-fn only)
│       └── kafka-consumer-health.test.ts                               (Task 13)

deploy/helm/service-chart/
├── templates/role.yaml                                                 # NEW (Task 15)
└── values.yaml                                                         # MODIFY (Task 15)

README.md                                                               # MODIFY (Task 16)
```

NO changes in 2.a to: services, canary-overlay.yaml, canary-ctl, e2e tests, Makefile.

---

## Task 1: Add `io.fabric8:kubernetes-client` dependency to lib-java

**Files:**
- Modify: `platform/lib-java/build.gradle.kts`

- [ ] **Step 1: Read current build.gradle.kts**

```bash
cat platform/lib-java/build.gradle.kts
```

- [ ] **Step 2: Add fabric8 kubernetes-client dep**

Locate the `dependencies { ... }` block. Add a single line:

```kotlin
implementation("io.fabric8:kubernetes-client:7.4.0")
```

inside the block, alongside other `implementation(...)` lines.

- [ ] **Step 3: Verify the build resolves the new dep**

```bash
./gradlew :platform:lib-java:dependencies --configuration compileClasspath 2>&1 | grep -i kubernetes-client
```

Expected: shows `io.fabric8:kubernetes-client:7.4.0` (or a transitive resolved version).

```bash
./gradlew :platform:lib-java:compileJava --quiet
```

Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add platform/lib-java/build.gradle.kts
git commit -m "$(cat <<'EOF'
feat(lib-java): add fabric8 kubernetes-client dep

Required for XCanaryPresenceWatcher to open a long-lived watch on
canary pods (Plan 2.a foundation). Used in subsequent tasks.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: lib-java — `XCanaryConsumerGroupIdResolver` (TDD)

**Files:**
- Create: `platform/lib-java/src/main/java/com/canary/platform/lib/XCanaryConsumerGroupIdResolver.java`
- Create: `platform/lib-java/src/test/java/com/canary/platform/lib/XCanaryConsumerGroupIdResolverTest.java`

- [ ] **Step 1: Write the failing test**

`platform/lib-java/src/test/java/com/canary/platform/lib/XCanaryConsumerGroupIdResolverTest.java`:

```java
package com.canary.platform.lib;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class XCanaryConsumerGroupIdResolverTest {

    @Test
    void resolvesStableSuffix() {
        XCanaryConsumerGroupIdResolver r = new XCanaryConsumerGroupIdResolver("stable");
        assertEquals("orders-events-consumers-stable", r.resolve("orders-events-consumers"));
    }

    @Test
    void resolvesCanarySuffix() {
        XCanaryConsumerGroupIdResolver r = new XCanaryConsumerGroupIdResolver("canary");
        assertEquals("orders-events-consumers-canary", r.resolve("orders-events-consumers"));
    }

    @Test
    void defaultsToStableWhenVersionNull() {
        XCanaryConsumerGroupIdResolver r = new XCanaryConsumerGroupIdResolver(null);
        assertEquals("base-stable", r.resolve("base"));
    }

    @Test
    void defaultsToStableWhenVersionBlank() {
        XCanaryConsumerGroupIdResolver r = new XCanaryConsumerGroupIdResolver("   ");
        assertEquals("base-stable", r.resolve("base"));
    }

    @Test
    void rejectsBlankBase() {
        XCanaryConsumerGroupIdResolver r = new XCanaryConsumerGroupIdResolver("stable");
        assertThrows(IllegalArgumentException.class, () -> r.resolve(""));
        assertThrows(IllegalArgumentException.class, () -> r.resolve(null));
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
./gradlew :platform:lib-java:test --tests XCanaryConsumerGroupIdResolverTest
```

Expected: FAIL — class does not exist.

- [ ] **Step 3: Write the implementation**

`platform/lib-java/src/main/java/com/canary/platform/lib/XCanaryConsumerGroupIdResolver.java`:

```java
package com.canary.platform.lib;

/**
 * Resolves a per-subset Kafka consumer group ID by appending the version
 * suffix to a base group ID. Used by services to ensure stable + canary
 * pods join different consumer groups.
 */
public class XCanaryConsumerGroupIdResolver {

    private final String version;

    public XCanaryConsumerGroupIdResolver(String version) {
        this.version = (version == null || version.isBlank()) ? "stable" : version.trim();
    }

    public String resolve(String baseGroupId) {
        if (baseGroupId == null || baseGroupId.isBlank()) {
            throw new IllegalArgumentException("baseGroupId must not be null or blank");
        }
        return baseGroupId + "-" + version;
    }

    public String version() {
        return version;
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
./gradlew :platform:lib-java:test --tests XCanaryConsumerGroupIdResolverTest
```

Expected: 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add platform/lib-java/src/main/java/com/canary/platform/lib/XCanaryConsumerGroupIdResolver.java \
        platform/lib-java/src/test/java/com/canary/platform/lib/XCanaryConsumerGroupIdResolverTest.java
git commit -m "$(cat <<'EOF'
feat(lib-java): XCanaryConsumerGroupIdResolver per-subset group ID

Appends version suffix (-stable / -canary) to a base group ID so
stable and canary pods join different Kafka consumer groups. Reads
the version from constructor (wired from canary.version /
VERSION env var by auto-config in a later task).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: lib-java — `XCanaryConsumeFilter` (TDD)

**Files:**
- Create: `platform/lib-java/src/main/java/com/canary/platform/lib/XCanaryConsumeFilter.java`
- Create: `platform/lib-java/src/test/java/com/canary/platform/lib/XCanaryConsumeFilterTest.java`

- [ ] **Step 1: Write the failing test**

`platform/lib-java/src/test/java/com/canary/platform/lib/XCanaryConsumeFilterTest.java`:

```java
package com.canary.platform.lib;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class XCanaryConsumeFilterTest {

    private Headers headers(String value) {
        Headers h = new RecordHeaders();
        if (value != null) {
            h.add("x-canary", value.getBytes(StandardCharsets.UTF_8));
        }
        return h;
    }

    @Test
    void canarySubset_processesCanaryFlagged() {
        XCanaryConsumeFilter f = new XCanaryConsumeFilter("canary", () -> true);
        assertTrue(f.shouldProcess(headers("true")));
    }

    @Test
    void canarySubset_skipsNonCanary() {
        XCanaryConsumeFilter f = new XCanaryConsumeFilter("canary", () -> true);
        assertFalse(f.shouldProcess(headers(null)));
        assertFalse(f.shouldProcess(headers("false")));
    }

    @Test
    void stableSubset_processesNonCanary() {
        XCanaryConsumeFilter f = new XCanaryConsumeFilter("stable", () -> true);
        assertTrue(f.shouldProcess(headers(null)));
        assertTrue(f.shouldProcess(headers("false")));
    }

    @Test
    void stableSubset_skipsCanaryWhenCanaryReady() {
        XCanaryConsumeFilter f = new XCanaryConsumeFilter("stable", () -> true);
        assertFalse(f.shouldProcess(headers("true")));
    }

    @Test
    void stableSubset_processesCanaryWhenCanaryAbsent_gracefulFallback() {
        XCanaryConsumeFilter f = new XCanaryConsumeFilter("stable", () -> false);
        assertTrue(f.shouldProcess(headers("true")));
    }

    @Test
    void unknownVersionTreatedAsStable() {
        XCanaryConsumeFilter f = new XCanaryConsumeFilter("v3", () -> true);
        assertTrue(f.shouldProcess(headers(null)));
        assertFalse(f.shouldProcess(headers("true")));
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
./gradlew :platform:lib-java:test --tests XCanaryConsumeFilterTest
```

Expected: FAIL — class does not exist.

- [ ] **Step 3: Write the implementation**

`platform/lib-java/src/main/java/com/canary/platform/lib/XCanaryConsumeFilter.java`:

```java
package com.canary.platform.lib;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;
import java.util.function.BooleanSupplier;

/**
 * Per-message filter applied at consume time.
 *   Canary subset: process if x-canary == "true". Skip otherwise.
 *   Stable subset: process if x-canary != "true" OR canary is not ready.
 * Header is the Kafka message header (NOT HTTP). The canaryReady supplier
 * is queried at evaluation time (push-updated by XCanaryPresenceWatcher).
 */
public class XCanaryConsumeFilter {

    private final String ownVersion;
    private final BooleanSupplier canaryReady;

    public XCanaryConsumeFilter(String ownVersion, BooleanSupplier canaryReady) {
        this.ownVersion = (ownVersion == null || ownVersion.isBlank()) ? "stable" : ownVersion.trim();
        this.canaryReady = canaryReady;
    }

    public boolean shouldProcess(Headers kafkaHeaders) {
        boolean carriesCanary = isCanaryFlagged(kafkaHeaders);
        if ("canary".equals(ownVersion)) {
            return carriesCanary;
        }
        // Treat any non-canary version (including "stable" or unknown) as stable behaviour.
        return !carriesCanary || !canaryReady.getAsBoolean();
    }

    static boolean isCanaryFlagged(Headers kafkaHeaders) {
        if (kafkaHeaders == null) return false;
        Header h = kafkaHeaders.lastHeader(XCanaryConstants.HEADER_NAME);
        if (h == null || h.value() == null) return false;
        String value = new String(h.value(), StandardCharsets.UTF_8);
        return XCanaryConstants.TRUE_VALUE.equals(value);
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
./gradlew :platform:lib-java:test --tests XCanaryConsumeFilterTest
```

Expected: 6 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add platform/lib-java/src/main/java/com/canary/platform/lib/XCanaryConsumeFilter.java \
        platform/lib-java/src/test/java/com/canary/platform/lib/XCanaryConsumeFilterTest.java
git commit -m "$(cat <<'EOF'
feat(lib-java): XCanaryConsumeFilter per-message decision logic

Stable: process if x-canary != true OR canary not ready (fallback).
Canary: process only if x-canary == true.
canaryReady is queried via BooleanSupplier (wired to
XCanaryPresenceWatcher in a later task).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: lib-java — `XCanaryConsumeContext` (TDD)

**Files:**
- Create: `platform/lib-java/src/main/java/com/canary/platform/lib/XCanaryConsumeContext.java`
- Create: `platform/lib-java/src/test/java/com/canary/platform/lib/XCanaryConsumeContextTest.java`

- [ ] **Step 1: Write the failing test**

`platform/lib-java/src/test/java/com/canary/platform/lib/XCanaryConsumeContextTest.java`:

```java
package com.canary.platform.lib;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class XCanaryConsumeContextTest {

    @AfterEach
    void clear() {
        XCanaryContext.clear();
    }

    private Headers headers(String value) {
        Headers h = new RecordHeaders();
        if (value != null) {
            h.add("x-canary", value.getBytes(StandardCharsets.UTF_8));
        }
        return h;
    }

    @Test
    void runsHandlerInsideCanaryContextWhenHeaderPresent() {
        AtomicBoolean observed = new AtomicBoolean(false);
        XCanaryConsumeContext.runWith(headers("true"), () -> observed.set(XCanaryContext.isCanary()));
        assertTrue(observed.get());
    }

    @Test
    void runsHandlerInsideStableContextWhenHeaderAbsent() {
        AtomicBoolean observed = new AtomicBoolean(true);
        XCanaryConsumeContext.runWith(headers(null), () -> observed.set(XCanaryContext.isCanary()));
        assertFalse(observed.get());
    }

    @Test
    void clearsContextAfterHandlerReturns() {
        XCanaryConsumeContext.runWith(headers("true"), () -> { /* no-op */ });
        assertFalse(XCanaryContext.isCanary());
    }

    @Test
    void clearsContextAfterHandlerThrows() {
        try {
            XCanaryConsumeContext.runWith(headers("true"), () -> { throw new RuntimeException("boom"); });
        } catch (RuntimeException ignored) { }
        assertFalse(XCanaryContext.isCanary());
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
./gradlew :platform:lib-java:test --tests XCanaryConsumeContextTest
```

Expected: FAIL — class does not exist.

- [ ] **Step 3: Write the implementation**

`platform/lib-java/src/main/java/com/canary/platform/lib/XCanaryConsumeContext.java`:

```java
package com.canary.platform.lib;

import org.apache.kafka.common.header.Headers;

/**
 * Helper for Kafka consume callbacks: opens an XCanaryContext frame
 * around the handler so outbound HTTP/Kafka/Restate calls inherit
 * x-canary via the existing Phase 1 interceptors. Restores the prior
 * context (always false at the start of a Kafka consume thread) on exit.
 */
public final class XCanaryConsumeContext {

    private XCanaryConsumeContext() {}

    public static void runWith(Headers kafkaHeaders, Runnable handler) {
        boolean canary = XCanaryConsumeFilter.isCanaryFlagged(kafkaHeaders);
        boolean prior = XCanaryContext.isCanary();
        XCanaryContext.set(canary);
        try {
            handler.run();
        } finally {
            XCanaryContext.set(prior);
            if (!prior) XCanaryContext.clear();
        }
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
./gradlew :platform:lib-java:test --tests XCanaryConsumeContextTest
```

Expected: 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add platform/lib-java/src/main/java/com/canary/platform/lib/XCanaryConsumeContext.java \
        platform/lib-java/src/test/java/com/canary/platform/lib/XCanaryConsumeContextTest.java
git commit -m "$(cat <<'EOF'
feat(lib-java): XCanaryConsumeContext header propagation in consume

Wraps a Kafka consume handler in an XCanaryContext frame so outbound
HTTP/Kafka/Restate calls inherit x-canary. Same primitive as the HTTP
request filter, just triggered by a Kafka consume callback.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: lib-java — `XCanaryPresenceWatcher` (k8s pod watch + pure-fn tests)

**Files:**
- Create: `platform/lib-java/src/main/java/com/canary/platform/lib/XCanaryPresenceWatcher.java`
- Create: `platform/lib-java/src/test/java/com/canary/platform/lib/XCanaryPresenceWatcherTest.java`

- [ ] **Step 1: Write the failing test**

`platform/lib-java/src/test/java/com/canary/platform/lib/XCanaryPresenceWatcherTest.java`:

```java
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
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
./gradlew :platform:lib-java:test --tests XCanaryPresenceWatcherTest
```

Expected: FAIL — class does not exist.

- [ ] **Step 3: Write the implementation**

`platform/lib-java/src/main/java/com/canary/platform/lib/XCanaryPresenceWatcher.java`:

```java
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
 * Watches canary pods (label selector app=<svc>,version=canary) in the
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
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
./gradlew :platform:lib-java:test --tests XCanaryPresenceWatcherTest
```

Expected: 7 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add platform/lib-java/src/main/java/com/canary/platform/lib/XCanaryPresenceWatcher.java \
        platform/lib-java/src/test/java/com/canary/platform/lib/XCanaryPresenceWatcherTest.java
git commit -m "$(cat <<'EOF'
feat(lib-java): XCanaryPresenceWatcher k8s pod watch

Opens a long-lived watch on canary pods (label selector
app=<svc>,version=canary) in the service's namespace via fabric8
KubernetesClient. Maintains an in-memory canaryReady atomic that
flips push-style as pods enter/leave Ready. Pure-function helpers
(computeCanaryReady, isPodReady) are unit-tested; watch lifecycle
is verified by manual operator testing after deploy.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: lib-java — `KafkaConsumerHealthIndicator` (TDD)

**Files:**
- Create: `platform/lib-java/src/main/java/com/canary/platform/lib/KafkaConsumerHealthIndicator.java`
- Create: `platform/lib-java/src/test/java/com/canary/platform/lib/KafkaConsumerHealthIndicatorTest.java`

- [ ] **Step 1: Write the failing test**

`platform/lib-java/src/test/java/com/canary/platform/lib/KafkaConsumerHealthIndicatorTest.java`:

```java
package com.canary.platform.lib;

import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaConsumerHealthIndicatorTest {

    @Test
    void initiallyOutOfService() {
        KafkaConsumerHealthIndicator h = new KafkaConsumerHealthIndicator(30_000);
        Health health = h.health();
        assertEquals(Status.OUT_OF_SERVICE, health.getStatus());
        assertTrue(health.getDetails().toString().toLowerCase().contains("no poll"));
    }

    @Test
    void upAfterRecentPoll() {
        KafkaConsumerHealthIndicator h = new KafkaConsumerHealthIndicator(30_000);
        h.recordPoll();
        assertEquals(Status.UP, h.health().getStatus());
    }

    @Test
    void outOfServiceWhenStale() {
        KafkaConsumerHealthIndicator h = new KafkaConsumerHealthIndicator(100); // 100ms timeout
        h.recordPoll();
        try { Thread.sleep(200); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        Health health = h.health();
        assertEquals(Status.OUT_OF_SERVICE, health.getStatus());
        assertTrue(health.getDetails().containsKey("staleSeconds"));
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
./gradlew :platform:lib-java:test --tests KafkaConsumerHealthIndicatorTest
```

Expected: FAIL — class does not exist.

- [ ] **Step 3: Write the implementation**

`platform/lib-java/src/main/java/com/canary/platform/lib/KafkaConsumerHealthIndicator.java`:

```java
package com.canary.platform.lib;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Spring Actuator HealthIndicator that reports the Kafka consumer's
 * liveness based on time since the last successful poll. Service code
 * (Plan 2.b) calls recordPoll() after each successful @KafkaListener
 * invocation. The readiness probe (which Actuator includes by default)
 * fails if this returns OUT_OF_SERVICE — kubelet then drops the pod
 * from the EndpointSlice.
 */
public class KafkaConsumerHealthIndicator implements HealthIndicator {

    private final long timeoutMs;
    private final AtomicLong lastPollMs = new AtomicLong(0);

    public KafkaConsumerHealthIndicator(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    public void recordPoll() {
        lastPollMs.set(System.currentTimeMillis());
    }

    @Override
    public Health health() {
        long last = lastPollMs.get();
        if (last == 0) {
            return Health.outOfService().withDetail("reason", "no poll yet").build();
        }
        long ageMs = System.currentTimeMillis() - last;
        if (ageMs > timeoutMs) {
            return Health.outOfService().withDetail("staleSeconds", ageMs / 1000).build();
        }
        return Health.up().withDetail("ageMs", ageMs).build();
    }
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
./gradlew :platform:lib-java:test --tests KafkaConsumerHealthIndicatorTest
```

Expected: 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add platform/lib-java/src/main/java/com/canary/platform/lib/KafkaConsumerHealthIndicator.java \
        platform/lib-java/src/test/java/com/canary/platform/lib/KafkaConsumerHealthIndicatorTest.java
git commit -m "$(cat <<'EOF'
feat(lib-java): KafkaConsumerHealthIndicator for readiness gating

Spring Actuator HealthIndicator that reports OUT_OF_SERVICE if no
successful Kafka poll within the configured timeout (default 30s).
Service code (Plan 2.b) calls recordPoll() after each @KafkaListener
invocation. /actuator/health/readiness aggregates this; failure
makes the readiness probe non-200, which drops the pod from the
EndpointSlice for stable's k8s watch to pick up.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: lib-java — Wire the new beans into `XCanaryAutoConfiguration`

**Files:**
- Modify: `platform/lib-java/src/main/java/com/canary/platform/lib/autoconfigure/XCanaryAutoConfiguration.java`

- [ ] **Step 1: Read the current auto-config**

```bash
cat platform/lib-java/src/main/java/com/canary/platform/lib/autoconfigure/XCanaryAutoConfiguration.java
```

- [ ] **Step 2: Add the four new bean definitions**

Replace `XCanaryAutoConfiguration.java` with:

```java
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestClient;

import java.util.function.Consumer;

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
            @Value("${canary.kafka-health-timeout-ms:30000}") long timeoutMs) {
        return new KafkaConsumerHealthIndicator(timeoutMs);
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
            XCanaryPresenceWatcher presenceWatcher) {
        return new XCanaryConsumeFilter(version, presenceWatcher::isCanaryReady);
    }
}
```

The watcher bean is conditional on `canary.presence-watcher.enabled` (default true) so unit tests can disable it. The `destroyMethod = "close"` ensures graceful shutdown when the Spring context closes.

- [ ] **Step 3: Run the full lib-java test suite**

```bash
./gradlew :platform:lib-java:test
```

Expected: all existing tests still pass + the new tests from Tasks 2-6 pass. Existing `XCanaryAutoConfigurationTest` may need updating to disable the presence watcher in unit context (it would try to connect to a non-existent k8s API).

If `XCanaryAutoConfigurationTest` fails because the presence watcher tries to connect, update it to set `canary.presence-watcher.enabled=false` in its test properties:

```java
// In XCanaryAutoConfigurationTest, add to the @ConfigurationContextRunner properties:
.withPropertyValues("canary.presence-watcher.enabled=false")
```

- [ ] **Step 4: Commit**

```bash
git add platform/lib-java/src/main/java/com/canary/platform/lib/autoconfigure/XCanaryAutoConfiguration.java \
        platform/lib-java/src/test/java/com/canary/platform/lib/XCanaryAutoConfigurationTest.java
git commit -m "$(cat <<'EOF'
feat(lib-java): wire Phase 2.a beans into XCanaryAutoConfiguration

Adds XCanaryConsumerGroupIdResolver, KafkaConsumerHealthIndicator,
XCanaryPresenceWatcher (conditional on canary.presence-watcher.enabled
to keep unit tests offline), XCanaryConsumeFilter. Watcher's
destroyMethod=close releases the k8s client on context shutdown.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 8: Add `@kubernetes/client-node` dependency to lib-node

**Files:**
- Modify: `platform/lib-node/package.json`

- [ ] **Step 1: Read current package.json**

```bash
cat platform/lib-node/package.json
```

- [ ] **Step 2: Add the dep**

Edit `platform/lib-node/package.json`. In the `dependencies` block, add:

```json
"@kubernetes/client-node": "^1.0.0"
```

(Pick the latest 1.x.x available; pnpm install will resolve.)

- [ ] **Step 3: Install**

```bash
pnpm install
```

Expected: pnpm resolves `@kubernetes/client-node` and its transitive deps (request-promise-native may be deprecated; library uses node-fetch internally now).

- [ ] **Step 4: Verify build**

```bash
pnpm --filter @canary/lib-node build
```

Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add platform/lib-node/package.json pnpm-lock.yaml
git commit -m "$(cat <<'EOF'
feat(lib-node): add @kubernetes/client-node dep

Required for xCanaryPresenceWatcher to open a long-lived watch on
canary pods (Plan 2.a foundation). Used in subsequent tasks.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 9: lib-node — `x-canary-consumer-group.ts` (TDD)

**Files:**
- Create: `platform/lib-node/src/x-canary-consumer-group.ts`
- Create: `platform/lib-node/src/__tests__/x-canary-consumer-group.test.ts`

- [ ] **Step 1: Write the failing test**

`platform/lib-node/src/__tests__/x-canary-consumer-group.test.ts`:

```typescript
import { describe, expect, it, afterEach } from "vitest";
import { resolveConsumerGroupId } from "../x-canary-consumer-group.js";

describe("resolveConsumerGroupId", () => {
  const original = process.env.VERSION;

  afterEach(() => {
    if (original === undefined) delete process.env.VERSION;
    else process.env.VERSION = original;
  });

  it("appends -stable when VERSION env is stable", () => {
    process.env.VERSION = "stable";
    expect(resolveConsumerGroupId("orders-events")).toBe("orders-events-stable");
  });

  it("appends -canary when VERSION env is canary", () => {
    process.env.VERSION = "canary";
    expect(resolveConsumerGroupId("orders-events")).toBe("orders-events-canary");
  });

  it("defaults to -stable when VERSION env is unset", () => {
    delete process.env.VERSION;
    expect(resolveConsumerGroupId("base")).toBe("base-stable");
  });

  it("defaults to -stable when VERSION is blank", () => {
    process.env.VERSION = "   ";
    expect(resolveConsumerGroupId("base")).toBe("base-stable");
  });

  it("throws on blank base", () => {
    expect(() => resolveConsumerGroupId("")).toThrow(/base.*blank/i);
    expect(() => resolveConsumerGroupId("   ")).toThrow(/base.*blank/i);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
pnpm --filter @canary/lib-node test
```

Expected: FAIL — module does not exist.

- [ ] **Step 3: Write the implementation**

`platform/lib-node/src/x-canary-consumer-group.ts`:

```typescript
/**
 * Resolves a per-subset Kafka consumer group ID by appending the version
 * suffix (-stable or -canary) to a base group ID. Reads VERSION from
 * process.env at call time; defaults to "stable".
 */
export function resolveConsumerGroupId(baseGroupId: string): string {
  if (typeof baseGroupId !== "string" || baseGroupId.trim().length === 0) {
    throw new Error("baseGroupId must not be blank");
  }
  const raw = process.env.VERSION;
  const version = raw && raw.trim().length > 0 ? raw.trim() : "stable";
  return `${baseGroupId}-${version}`;
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
pnpm --filter @canary/lib-node test
```

Expected: 5 new tests PASS plus all existing.

- [ ] **Step 5: Commit**

```bash
git add platform/lib-node/src/x-canary-consumer-group.ts \
        platform/lib-node/src/__tests__/x-canary-consumer-group.test.ts
git commit -m "$(cat <<'EOF'
feat(lib-node): resolveConsumerGroupId per-subset group ID

Mirrors XCanaryConsumerGroupIdResolver from lib-java. Appends -stable
or -canary based on VERSION env var.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 10: lib-node — `x-canary-consume-filter.ts` (TDD)

**Files:**
- Create: `platform/lib-node/src/x-canary-consume-filter.ts`
- Create: `platform/lib-node/src/__tests__/x-canary-consume-filter.test.ts`

- [ ] **Step 1: Write the failing test**

`platform/lib-node/src/__tests__/x-canary-consume-filter.test.ts`:

```typescript
import { describe, expect, it } from "vitest";
import { shouldProcess, isCanaryFlagged } from "../x-canary-consume-filter.js";

function headers(value: string | undefined): Record<string, Buffer> {
  if (value === undefined) return {};
  return { "x-canary": Buffer.from(value) };
}

describe("isCanaryFlagged", () => {
  it("true for x-canary=true", () => {
    expect(isCanaryFlagged(headers("true"))).toBe(true);
  });
  it("false for x-canary=false", () => {
    expect(isCanaryFlagged(headers("false"))).toBe(false);
  });
  it("false when header absent", () => {
    expect(isCanaryFlagged({})).toBe(false);
    expect(isCanaryFlagged(undefined)).toBe(false);
  });
});

describe("shouldProcess", () => {
  it("canary subset: processes canary-flagged", () => {
    expect(shouldProcess(headers("true"), "canary", () => true)).toBe(true);
  });
  it("canary subset: skips non-canary", () => {
    expect(shouldProcess(headers(undefined), "canary", () => true)).toBe(false);
    expect(shouldProcess(headers("false"), "canary", () => true)).toBe(false);
  });
  it("stable subset: processes non-canary", () => {
    expect(shouldProcess(headers(undefined), "stable", () => true)).toBe(true);
    expect(shouldProcess(headers("false"), "stable", () => true)).toBe(true);
  });
  it("stable subset: skips canary when canary ready", () => {
    expect(shouldProcess(headers("true"), "stable", () => true)).toBe(false);
  });
  it("stable subset: processes canary when canary absent (graceful fallback)", () => {
    expect(shouldProcess(headers("true"), "stable", () => false)).toBe(true);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
pnpm --filter @canary/lib-node test
```

Expected: FAIL — module does not exist.

- [ ] **Step 3: Write the implementation**

`platform/lib-node/src/x-canary-consume-filter.ts`:

```typescript
import { X_CANARY_HEADER, X_CANARY_TRUE } from "./x-canary-constants.js";

/**
 * Kafka message headers come as Record<string, Buffer | undefined> from kafkajs.
 */
export type KafkaConsumeHeaders = Record<string, Buffer | undefined> | undefined;

export function isCanaryFlagged(headers: KafkaConsumeHeaders): boolean {
  if (!headers) return false;
  const raw = headers[X_CANARY_HEADER];
  if (!raw) return false;
  return raw.toString("utf8") === X_CANARY_TRUE;
}

/**
 * Per-message filter applied at consume time.
 *   Canary subset: process if x-canary == "true". Skip otherwise.
 *   Stable subset (or unknown): process if x-canary != "true" OR canary not ready.
 */
export function shouldProcess(
  headers: KafkaConsumeHeaders,
  ownVersion: string,
  isCanaryReady: () => boolean,
): boolean {
  const carriesCanary = isCanaryFlagged(headers);
  if (ownVersion === "canary") {
    return carriesCanary;
  }
  return !carriesCanary || !isCanaryReady();
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
pnpm --filter @canary/lib-node test
```

Expected: 8 new tests PASS plus prior.

- [ ] **Step 5: Commit**

```bash
git add platform/lib-node/src/x-canary-consume-filter.ts \
        platform/lib-node/src/__tests__/x-canary-consume-filter.test.ts
git commit -m "$(cat <<'EOF'
feat(lib-node): shouldProcess + isCanaryFlagged consume filter

Mirrors XCanaryConsumeFilter from lib-java. Canary processes only
canary-flagged events; stable processes all non-canary, plus
canary-flagged when canary is not ready (graceful fallback).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 11: lib-node — `x-canary-consume-context.ts` (TDD)

**Files:**
- Create: `platform/lib-node/src/x-canary-consume-context.ts`
- Create: `platform/lib-node/src/__tests__/x-canary-consume-context.test.ts`

- [ ] **Step 1: Write the failing test**

`platform/lib-node/src/__tests__/x-canary-consume-context.test.ts`:

```typescript
import { describe, expect, it } from "vitest";
import { runWithCanaryFromHeaders } from "../x-canary-consume-context.js";
import { isCanary } from "../x-canary-context.js";

function headers(value: string | undefined): Record<string, Buffer> {
  if (value === undefined) return {};
  return { "x-canary": Buffer.from(value) };
}

describe("runWithCanaryFromHeaders", () => {
  it("runs handler in canary context when header is true", async () => {
    let observed = false;
    await runWithCanaryFromHeaders(headers("true"), async () => {
      observed = isCanary();
    });
    expect(observed).toBe(true);
  });

  it("runs handler in stable context when header is absent", async () => {
    let observed = true;
    await runWithCanaryFromHeaders(headers(undefined), async () => {
      observed = isCanary();
    });
    expect(observed).toBe(false);
  });

  it("propagates handler return value", async () => {
    const r = await runWithCanaryFromHeaders(headers("true"), async () => 42);
    expect(r).toBe(42);
  });

  it("propagates handler exceptions", async () => {
    await expect(
      runWithCanaryFromHeaders(headers("true"), async () => { throw new Error("boom"); }),
    ).rejects.toThrow(/boom/);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
pnpm --filter @canary/lib-node test
```

Expected: FAIL — module does not exist.

- [ ] **Step 3: Write the implementation**

`platform/lib-node/src/x-canary-consume-context.ts`:

```typescript
import { runWithCanary } from "./x-canary-context.js";
import { isCanaryFlagged, type KafkaConsumeHeaders } from "./x-canary-consume-filter.js";

/**
 * Wraps a Kafka consume handler in an x-canary context frame so outbound
 * HTTP/Kafka/Restate calls inherit x-canary via existing Phase 1
 * interceptors. Same primitive as the HTTP middleware, just triggered
 * by a Kafka consume callback instead of an HTTP request.
 */
export async function runWithCanaryFromHeaders<T>(
  headers: KafkaConsumeHeaders,
  handler: () => Promise<T>,
): Promise<T> {
  const canary = isCanaryFlagged(headers);
  return runWithCanary(canary, handler);
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
pnpm --filter @canary/lib-node test
```

Expected: 4 new tests PASS plus prior.

- [ ] **Step 5: Commit**

```bash
git add platform/lib-node/src/x-canary-consume-context.ts \
        platform/lib-node/src/__tests__/x-canary-consume-context.test.ts
git commit -m "$(cat <<'EOF'
feat(lib-node): runWithCanaryFromHeaders consume context wrap

Mirrors XCanaryConsumeContext from lib-java. Wraps a Kafka consume
handler in a runWithCanary frame so outbound HTTP/Kafka/Restate
calls inherit x-canary via existing Phase 1 axios/Kafka/Restate
interceptors.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 12: lib-node — `x-canary-presence-watcher.ts` (k8s client + pure-fn tests)

**Files:**
- Create: `platform/lib-node/src/x-canary-presence-watcher.ts`
- Create: `platform/lib-node/src/__tests__/x-canary-presence-watcher.test.ts`

- [ ] **Step 1: Write the failing test**

`platform/lib-node/src/__tests__/x-canary-presence-watcher.test.ts`:

```typescript
import { describe, expect, it } from "vitest";
import type { V1Pod } from "@kubernetes/client-node";
import { computeCanaryReady, isPodReady } from "../x-canary-presence-watcher.js";

function pod(name: string, ready: boolean | null): V1Pod {
  const p: V1Pod = { metadata: { name } };
  if (ready !== null) {
    p.status = {
      conditions: [{ type: "Ready", status: ready ? "True" : "False" }],
    };
  }
  return p;
}

describe("isPodReady", () => {
  it("true when Ready condition is True", () => {
    expect(isPodReady(pod("p", true))).toBe(true);
  });
  it("false when Ready condition is False", () => {
    expect(isPodReady(pod("p", false))).toBe(false);
  });
  it("false when status missing", () => {
    expect(isPodReady(pod("p", null))).toBe(false);
  });
  it("false when conditions missing", () => {
    const p: V1Pod = { metadata: { name: "p" }, status: {} };
    expect(isPodReady(p)).toBe(false);
  });
});

describe("computeCanaryReady", () => {
  it("false on empty list", () => {
    expect(computeCanaryReady([])).toBe(false);
  });
  it("true when at least one pod is Ready", () => {
    expect(computeCanaryReady([pod("p1", false), pod("p2", true)])).toBe(true);
  });
  it("false when all not Ready", () => {
    expect(computeCanaryReady([pod("p1", false), pod("p2", false)])).toBe(false);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
pnpm --filter @canary/lib-node test
```

Expected: FAIL — module does not exist.

- [ ] **Step 3: Write the implementation**

`platform/lib-node/src/x-canary-presence-watcher.ts`:

```typescript
import { CoreV1Api, KubeConfig, type V1Pod, Watch } from "@kubernetes/client-node";

/**
 * Watches canary pods (label selector app=<svc>,version=canary) in the
 * service's namespace. Maintains an in-memory canaryReady flag that flips
 * push-style as pods enter/leave Ready.
 */
export class XCanaryPresenceWatcher {
  private readonly podReady = new Map<string, boolean>();
  private canaryReady = false;
  private watchRequest: { abort: () => void } | null = null;
  private closed = false;

  constructor(
    private readonly namespace: string,
    private readonly serviceName: string,
    private readonly kc: KubeConfig = defaultInClusterKubeConfig(),
  ) {}

  isCanaryReady(): boolean {
    return this.canaryReady;
  }

  async start(): Promise<void> {
    const labelSelector = `app=${this.serviceName},version=canary`;
    const coreApi = this.kc.makeApiClient(CoreV1Api);

    // Initial list to populate state.
    const list = await coreApi.listNamespacedPod(
      this.namespace,
      undefined, undefined, undefined, undefined,
      labelSelector,
    );
    for (const p of list.body.items) {
      const name = p.metadata?.name;
      if (name) this.podReady.set(name, isPodReady(p));
    }
    this.recomputeFlag();

    // Open long-lived watch.
    const watch = new Watch(this.kc);
    this.watchRequest = await watch.watch(
      `/api/v1/namespaces/${this.namespace}/pods`,
      { labelSelector },
      (type: string, obj: V1Pod) => {
        const name = obj.metadata?.name;
        if (!name) return;
        if (type === "DELETED") {
          this.podReady.delete(name);
        } else {
          this.podReady.set(name, isPodReady(obj));
        }
        this.recomputeFlag();
      },
      (_err) => {
        // Reconnect on close (unless we shut down).
        if (!this.closed) {
          setTimeout(() => { void this.start(); }, 1000);
        }
      },
    );
  }

  close(): void {
    this.closed = true;
    if (this.watchRequest) {
      try { this.watchRequest.abort(); } catch { /* ignore */ }
    }
  }

  private recomputeFlag(): void {
    let any = false;
    for (const ready of this.podReady.values()) {
      if (ready) { any = true; break; }
    }
    this.canaryReady = any;
  }
}

function defaultInClusterKubeConfig(): KubeConfig {
  const kc = new KubeConfig();
  kc.loadFromCluster();
  return kc;
}

/** Pure function for unit testing. */
export function computeCanaryReady(pods: V1Pod[]): boolean {
  return pods.some(isPodReady);
}

/** Pure function for unit testing. */
export function isPodReady(pod: V1Pod): boolean {
  if (!pod.status?.conditions) return false;
  return pod.status.conditions.some((c) => c.type === "Ready" && c.status === "True");
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
pnpm --filter @canary/lib-node test
```

Expected: 7 new tests PASS plus prior.

- [ ] **Step 5: Commit**

```bash
git add platform/lib-node/src/x-canary-presence-watcher.ts \
        platform/lib-node/src/__tests__/x-canary-presence-watcher.test.ts
git commit -m "$(cat <<'EOF'
feat(lib-node): xCanaryPresenceWatcher k8s pod watch

Opens a long-lived watch on canary pods (label selector
app=<svc>,version=canary) in the service's namespace via
@kubernetes/client-node. Maintains an in-memory canaryReady flag
that flips push-style as pods enter/leave Ready. Pure-function
helpers (computeCanaryReady, isPodReady) are unit-tested; watch
lifecycle is verified by manual operator testing after deploy.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 13: lib-node — `kafka-consumer-health.ts` (TDD)

**Files:**
- Create: `platform/lib-node/src/kafka-consumer-health.ts`
- Create: `platform/lib-node/src/__tests__/kafka-consumer-health.test.ts`

- [ ] **Step 1: Write the failing test**

`platform/lib-node/src/__tests__/kafka-consumer-health.test.ts`:

```typescript
import { describe, expect, it } from "vitest";
import { createKafkaHealthState } from "../kafka-consumer-health.js";

describe("kafka-consumer-health", () => {
  it("initially not healthy (no poll yet)", () => {
    const s = createKafkaHealthState(30_000);
    expect(s.isHealthy()).toBe(false);
    expect(s.report().ok).toBe(false);
    expect(s.report().reason).toMatch(/no poll/i);
  });

  it("healthy after recordPoll", () => {
    const s = createKafkaHealthState(30_000);
    s.recordPoll();
    expect(s.isHealthy()).toBe(true);
    expect(s.report().ok).toBe(true);
  });

  it("not healthy when stale beyond timeout", async () => {
    const s = createKafkaHealthState(50);
    s.recordPoll();
    await new Promise((r) => setTimeout(r, 100));
    expect(s.isHealthy()).toBe(false);
    expect(s.report().reason).toMatch(/stale/i);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
pnpm --filter @canary/lib-node test
```

Expected: FAIL — module does not exist.

- [ ] **Step 3: Write the implementation**

`platform/lib-node/src/kafka-consumer-health.ts`:

```typescript
export interface KafkaHealthReport {
  ok: boolean;
  reason?: string;
  ageMs?: number;
}

export interface KafkaHealthState {
  recordPoll(): void;
  isHealthy(): boolean;
  report(): KafkaHealthReport;
}

/**
 * Tracks last successful Kafka poll timestamp. Used by service /health
 * routes to fail readiness when the consumer is disconnected/stale.
 * Default timeout 30s.
 */
export function createKafkaHealthState(timeoutMs: number = 30_000): KafkaHealthState {
  let lastPollMs = 0;
  return {
    recordPoll() {
      lastPollMs = Date.now();
    },
    isHealthy() {
      if (lastPollMs === 0) return false;
      return Date.now() - lastPollMs <= timeoutMs;
    },
    report() {
      if (lastPollMs === 0) {
        return { ok: false, reason: "no poll yet" };
      }
      const ageMs = Date.now() - lastPollMs;
      if (ageMs > timeoutMs) {
        return { ok: false, reason: `stale ${Math.floor(ageMs / 1000)}s`, ageMs };
      }
      return { ok: true, ageMs };
    },
  };
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
pnpm --filter @canary/lib-node test
```

Expected: 3 new tests PASS plus prior.

- [ ] **Step 5: Commit**

```bash
git add platform/lib-node/src/kafka-consumer-health.ts \
        platform/lib-node/src/__tests__/kafka-consumer-health.test.ts
git commit -m "$(cat <<'EOF'
feat(lib-node): createKafkaHealthState for readiness gating

Mirrors KafkaConsumerHealthIndicator from lib-java. Tracks last
successful Kafka poll; reports ok=false when stale (default 30s).
Service /health routes (Plan 2.b) call recordPoll() after each
batch and include report() in their /health response.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 14: lib-node — `index.ts` re-exports

**Files:**
- Modify: `platform/lib-node/src/index.ts`

- [ ] **Step 1: Read current index.ts**

```bash
cat platform/lib-node/src/index.ts
```

- [ ] **Step 2: Append the new re-exports**

Append to the end of `platform/lib-node/src/index.ts`:

```typescript
export * from "./x-canary-consumer-group.js";
export * from "./x-canary-consume-filter.js";
export * from "./x-canary-consume-context.js";
export * from "./x-canary-presence-watcher.js";
export * from "./kafka-consumer-health.js";
```

- [ ] **Step 3: Build to verify all re-exports resolve**

```bash
pnpm --filter @canary/lib-node build
```

Expected: clean.

- [ ] **Step 4: Run the full lib-node test suite**

```bash
pnpm --filter @canary/lib-node test
```

Expected: all tests pass (Tasks 9-13's new tests + everything from before).

- [ ] **Step 5: Commit**

```bash
git add platform/lib-node/src/index.ts
git commit -m "feat(lib-node): re-export Phase 2.a modules from package index"
```

(HEREDOC + Co-Authored-By footer.)

---

## Task 15: Helm chart — `role.yaml` + `values.yaml`

**Files:**
- Create: `deploy/helm/service-chart/templates/role.yaml`
- Modify: `deploy/helm/service-chart/values.yaml`

- [ ] **Step 1: Add `canaryWatch.enabled` default to values.yaml**

Read current `deploy/helm/service-chart/values.yaml`. Add a new top-level key (alphabetical order or alongside other feature flags):

```yaml
canaryWatch:
  enabled: true
```

- [ ] **Step 2: Create `role.yaml` template**

`deploy/helm/service-chart/templates/role.yaml`:

```yaml
{{- if .Values.canaryWatch.enabled }}
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: {{ include "service-chart.resourceName" . }}-canary-watch
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "service-chart.labels" . | nindent 4 }}
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: {{ include "service-chart.resourceName" . }}-canary-watch
  namespace: {{ .Release.Namespace }}
  labels:
    {{- include "service-chart.labels" . | nindent 4 }}
subjects:
  - kind: ServiceAccount
    name: {{ .Values.serviceName }}
    namespace: {{ .Release.Namespace }}
roleRef:
  kind: Role
  name: {{ include "service-chart.resourceName" . }}-canary-watch
  apiGroup: rbac.authorization.k8s.io
{{- end }}
```

- [ ] **Step 3: Verify chart renders for stable and canary**

```bash
helm template test deploy/helm/service-chart -f deploy/helm/values/payment-service.yaml | grep -A2 "kind: Role" | head -10
helm template test deploy/helm/service-chart -f deploy/helm/values/payment-service.yaml | grep -A2 "kind: RoleBinding" | head -10
```

Expected: both Role and RoleBinding render with the right name (`payment-service-canary-watch`) and reference the `payment-service` ServiceAccount.

```bash
helm template test deploy/helm/service-chart -f deploy/helm/values/payment-service.yaml --set canaryWatch.enabled=false | grep -i role
```

Expected: no output (template skipped when feature flag is false).

- [ ] **Step 4: Commit**

```bash
git add deploy/helm/service-chart/templates/role.yaml \
        deploy/helm/service-chart/values.yaml
git commit -m "$(cat <<'EOF'
feat(helm): Role + RoleBinding for pods get/list/watch RBAC

Required by lib-java's XCanaryPresenceWatcher and lib-node's
xCanaryPresenceWatcher to open a long-lived k8s watch on canary
pods. Conditional on .Values.canaryWatch.enabled (default true).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 16: README — Plan 2.a section

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Append a section to README**

Append to the end of `README.md`:

```markdown

## Plan 2.a — Kafka canary consumer foundation (complete)

Phase 2.a ships the **lib code + Helm RBAC** for canary-aware Kafka consumption. NOT yet wired into services — that lands in Plan 2.b. After 2.a merges, the cluster's behavior is unchanged; the new lib code sits unused until 2.b consumes it.

### What landed

**lib-java** (`platform/lib-java/`):
- `XCanaryConsumerGroupIdResolver` — appends `-stable` / `-canary` to base group IDs so each subset joins its own consumer group
- `XCanaryConsumeFilter` — per-message decision: canary processes only `x-canary=true`; stable processes all non-canary plus canary-flagged when canary is absent (graceful fallback)
- `XCanaryConsumeContext.runWith(headers, handler)` — wraps a Kafka consume callback in an `XCanaryContext` frame so outbound HTTP/Kafka/Restate calls inherit `x-canary`
- `XCanaryPresenceWatcher` — opens a long-lived k8s watch on `Pods` matching `app=<svc>,version=canary`; maintains an atomic `canaryReady` flag updated push-style by watch events
- `KafkaConsumerHealthIndicator` — Spring Actuator HealthIndicator that reports OUT_OF_SERVICE if no successful Kafka poll within 30s (configurable)

**lib-node** (`platform/lib-node/`): equivalent set — `resolveConsumerGroupId`, `shouldProcess`, `runWithCanaryFromHeaders`, `XCanaryPresenceWatcher`, `createKafkaHealthState`.

**Helm chart** (`deploy/helm/service-chart/`): new `Role` + `RoleBinding` granting the service's ServiceAccount `pods` get/list/watch in its namespace. Conditional on `.Values.canaryWatch.enabled` (default `true`).

### How presence detection works

Each stable pod opens a long-lived watch on canary-version pods in its namespace. K8s pushes events as canary deploys/rolls back/crashes — typical detection lag is <1s. Hot-path consume filter is an O(1) atomic flag read; no per-message API calls.

The canary pod's readiness probe is gated on Kafka consumer health (Plan 2.b adds the wiring to call `recordPoll()` after each successful consume). When the canary's consumer disconnects, the readiness probe fails → kubelet drops the pod from the EndpointSlice → stable's pod watch sees the pod transition to `Ready=False` → stable's flag flips → stable processes the next canary-flagged event.

### Operator smoke check (after deploy)

```bash
# RBAC works:
kubectl auth can-i watch pods -n services --as=system:serviceaccount:services:payment-service
# Expected: yes

# HealthIndicator surfaces (Java services with Actuator):
kubectl -n services exec deploy/payment-service-stable -- curl -s localhost:8081/actuator/health | jq '.components | keys'
# Expected: includes "kafkaConsumer" (Spring auto-discovers HealthIndicator beans by class name → camelCase)
```

Next: Plan 2.b wires these abstractions into all 5 services, flips `KAFKA_CONSUMERS_ENABLED=true` in canary-overlay, and adds Phase 2 e2e scenarios K1–K5.
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs(readme): add Plan 2.a section (Kafka canary consumer foundation)"
```

(HEREDOC + footer.)

---

## Task 17: Operator manual verification

**Files:** none modified.

This requires a real cluster. Operator runs after the branch merges.

- [ ] **Step 1: Build everything**

```bash
make build-services
```

Expected: clean (lib changes compile; services still build with the new lib but don't consume the new abstractions).

- [ ] **Step 2: Run all unit tests**

```bash
make verify
```

Expected: all unit tests pass — existing Phase 1 tests + the ~30 new Phase 2.a lib tests.

- [ ] **Step 3: Refresh images and redeploy**

```bash
make build-images && make load-images
make deploy-services
```

Expected: deploys cleanly. Helm chart now creates a Role + RoleBinding per service.

- [ ] **Step 4: Verify RBAC for one service**

```bash
kubectl auth can-i watch pods -n services --as=system:serviceaccount:services:payment-service
```

Expected: `yes`.

- [ ] **Step 5: Verify HealthIndicator visible (Java service)**

```bash
kubectl -n services exec deploy/payment-service-stable -- curl -s localhost:8081/actuator/health | jq '.components | keys'
```

Expected: includes `"kafkaConsumer"` and any other indicators (db, ping). Note: status will be OUT_OF_SERVICE because no service code calls `recordPoll()` yet — that's Plan 2.b.

- [ ] **Step 6: Verify nothing else broke**

```bash
make smoke-services
make smoke-canary
make e2e
```

Expected: Phase 1 smoke + Phase 1 e2e all still pass. The lib additions and RBAC don't change runtime behavior because no service consumes them.

- [ ] **Step 7: No commit — verification task only**

If anything regresses, return to the failing task and fix.

---

## Self-review checklist

- **Spec coverage.** Each spec section maps to plan tasks: per-subset group IDs → Tasks 2 + 9; consume filter → Tasks 3 + 10; consume context → Tasks 4 + 11; presence watcher → Tasks 5 + 12; Kafka health → Tasks 6 + 13; auto-config wiring → Task 7; lib-node re-exports → Task 14; Helm RBAC → Task 15; README → Task 16; operator verification → Task 17.
- **Placeholders.** None. All file contents are concrete code or YAML.
- **Type/name consistency.** `XCanaryConsumerGroupIdResolver`/`resolveConsumerGroupId`, `XCanaryConsumeFilter`/`shouldProcess`, `XCanaryConsumeContext`/`runWithCanaryFromHeaders`, `XCanaryPresenceWatcher` (both langs), `KafkaConsumerHealthIndicator`/`createKafkaHealthState` named consistently across plan tasks. `canary.presence-watcher.enabled` Spring property name and `canaryWatch.enabled` Helm value name are different strings — that's intentional (Spring uses dotted notation; Helm uses YAML conventions).
- **TDD discipline.** Tasks 2-6, 9-13 follow TDD with failing-test-first. Tasks 1, 7, 8, 14, 15, 16 are infrastructure/wiring with no logic to test directly (verified by downstream tasks or by manual operator verification in Task 17).
- **Frequent commits.** 16 commits across Tasks 1-16 (Task 17 makes none).

---

## Done when

- All unit tests pass: `make verify` runs cleanly with the new lib tests.
- `pnpm --filter @canary/lib-node build` and `./gradlew :platform:lib-java:build` are clean.
- Helm chart renders cleanly with the Role + RoleBinding (`helm template test deploy/helm/service-chart -f deploy/helm/values/payment-service.yaml | grep -E 'Role|RoleBinding'`).
- README documents Plan 2.a.
- All commits in this task list are present on `claude/phase-2.a-kafka-canary-foundation`.
