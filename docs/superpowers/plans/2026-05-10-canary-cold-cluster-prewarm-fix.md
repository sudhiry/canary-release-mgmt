# Cold-cluster pre-warm fix — heartbeat-based Kafka readiness probe

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the "first poll received" Kafka readiness signal with "consumer joined + heartbeat fresh" so a brand-new canary pod reaches Ready on a cold cluster (no traffic) without `make pre-warm`, while preserving the K5 SIGSTOP-detection contract.

**Architecture:** Two libraries change (`platform/lib-java`, `platform/lib-node`). Three Java services (audit, payment, inventory) and two Node services (order, notification) consume the new signal. The Java probe reads the kafka-clients consumer metric `last-heartbeat-seconds-ago` plus partition-assignment app events. The Node probe subscribes to kafkajs `consumer.events.HEARTBEAT/GROUP_JOIN/REBALANCING/DISCONNECT`. Default heartbeat-staleness threshold = 15s. `make pre-warm` is kept but de-emphasized in docs.

**Tech Stack:** Java 21, Spring Boot 4.0.4, spring-kafka, kafka-clients 4.1.2, JUnit 5, Node 22, kafkajs 2.2.4, vitest, kind/Helm.

**Spec:** [docs/superpowers/specs/2026-05-10-canary-cold-cluster-prewarm-fix-design.md](../specs/2026-05-10-canary-cold-cluster-prewarm-fix-design.md)

---

## File map

| Layer | Path | Action |
|---|---|---|
| Java lib | `platform/lib-java/src/main/java/com/canary/platform/lib/KafkaConsumerHealthIndicator.java` | Rewrite |
| Java lib | `platform/lib-java/src/test/java/com/canary/platform/lib/KafkaConsumerHealthIndicatorTest.java` | Rewrite |
| Java lib | `platform/lib-java/src/main/java/com/canary/platform/lib/autoconfigure/XCanaryAutoConfiguration.java` | Modify (rewire indicator + add app event listeners + heartbeat supplier bean) |
| Java service | `services/audit-service/src/main/java/com/canary/audit/kafka/AuditKafkaListener.java` | Modify (drop indicator field + recordPoll call) |
| Java service | `services/payment-service/src/main/java/com/canary/payment/kafka/PaymentKafkaListener.java` | Modify (same) |
| Java service | `services/inventory-service/src/main/java/com/canary/inventory/kafka/InventoryKafkaListener.java` | Modify (same) |
| Java service tests | `services/{audit,payment,inventory}-service/src/test/java/.../*KafkaListenerGatingTest.java` | Modify (drop indicator from constructor) |
| Node lib | `platform/lib-node/src/kafka-consumer-health.ts` | Rewrite |
| Node lib tests | `platform/lib-node/src/__tests__/kafka-consumer-health.test.ts` | Rewrite |
| Node service | `services/order-service/src/kafka.ts` | Modify (add event subscriptions, remove recordPoll) |
| Node service | `services/notification-service/src/kafka.ts` | Modify (same) |
| Node service config | `services/order-service/src/config.ts` | Modify (rename + alias) |
| Node service config | `services/notification-service/src/config.ts` | Modify (same) |
| Node service tests | `services/{order,notification}-service/src/__tests__/{config,kafka,http}.test.ts` | Modify |
| Tooling | `Makefile` | Modify (de-emphasize pre-warm target description) |
| Tooling | `deploy/services/deploy.sh` | Modify (drop post-deploy reminder) |
| Tooling | `deploy/services/pre-warm.sh` | Modify (reframe header comments) |
| Helm | `deploy/helm/values/canary-overlay.yaml` | Modify (comment update) |
| Docs | `README.md`, `docs/operations.md`, `docs/canary-mechanics.md`, `docs/development.md`, `docs/history.md` | Modify |
| E2E | `tests/e2e/k6-cold-cluster-bringup.test.ts` | Create |

---

## Task 1: Java lib — failing tests for new state machine

**Files:**
- Modify: `platform/lib-java/src/test/java/com/canary/platform/lib/KafkaConsumerHealthIndicatorTest.java`

- [ ] **Step 1.1: Replace the test file with the new state-machine tests**

```java
package com.canary.platform.lib;

import org.junit.jupiter.api.Test;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;

import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaConsumerHealthIndicatorTest {

    private AtomicReference<OptionalLong> ageRef(OptionalLong initial) {
        return new AtomicReference<>(initial);
    }

    @Test
    void notAssignedReturnsDown() {
        AtomicReference<OptionalLong> age = ageRef(OptionalLong.of(1000));
        KafkaConsumerHealthIndicator h = new KafkaConsumerHealthIndicator(15_000, age::get);
        Health health = h.health();
        assertEquals(Status.OUT_OF_SERVICE, health.getStatus());
        assertTrue(health.getDetails().toString().toLowerCase().contains("no partitions assigned"));
    }

    @Test
    void assignedButNoHeartbeatReturnsDown() {
        AtomicReference<OptionalLong> age = ageRef(OptionalLong.empty());
        KafkaConsumerHealthIndicator h = new KafkaConsumerHealthIndicator(15_000, age::get);
        h.onPartitionsAssigned();
        Health health = h.health();
        assertEquals(Status.OUT_OF_SERVICE, health.getStatus());
        assertTrue(health.getDetails().toString().toLowerCase().contains("no heartbeat yet"));
    }

    @Test
    void assignedAndFreshHeartbeatReturnsUp() {
        AtomicReference<OptionalLong> age = ageRef(OptionalLong.of(1000));
        KafkaConsumerHealthIndicator h = new KafkaConsumerHealthIndicator(15_000, age::get);
        h.onPartitionsAssigned();
        Health health = h.health();
        assertEquals(Status.UP, health.getStatus());
        assertTrue(health.getDetails().containsKey("heartbeatAgeMs"));
    }

    @Test
    void assignedAndStaleHeartbeatReturnsDown() {
        AtomicReference<OptionalLong> age = ageRef(OptionalLong.of(20_000));
        KafkaConsumerHealthIndicator h = new KafkaConsumerHealthIndicator(15_000, age::get);
        h.onPartitionsAssigned();
        Health health = h.health();
        assertEquals(Status.OUT_OF_SERVICE, health.getStatus());
        assertTrue(health.getDetails().containsKey("heartbeatStaleMs"));
    }

    @Test
    void revokedAfterAssignedReturnsDown() {
        AtomicReference<OptionalLong> age = ageRef(OptionalLong.of(1000));
        KafkaConsumerHealthIndicator h = new KafkaConsumerHealthIndicator(15_000, age::get);
        h.onPartitionsAssigned();
        assertEquals(Status.UP, h.health().getStatus());
        h.onPartitionsRevoked();
        assertEquals(Status.OUT_OF_SERVICE, h.health().getStatus());
    }
}
```

- [ ] **Step 1.2: Run the tests to confirm they fail**

Run: `./gradlew :platform:lib-java:test --tests com.canary.platform.lib.KafkaConsumerHealthIndicatorTest`
Expected: FAIL — compilation errors (constructor signature changed, methods missing).

---

## Task 2: Java lib — implement new KafkaConsumerHealthIndicator

**Files:**
- Modify: `platform/lib-java/src/main/java/com/canary/platform/lib/KafkaConsumerHealthIndicator.java`

- [ ] **Step 2.1: Replace the implementation**

```java
package com.canary.platform.lib;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class KafkaConsumerHealthIndicator implements HealthIndicator {

    private final long heartbeatStaleMs;
    private final Supplier<OptionalLong> lastHeartbeatAgeMsSupplier;
    private final AtomicBoolean assigned = new AtomicBoolean(false);

    public KafkaConsumerHealthIndicator(long heartbeatStaleMs,
                                        Supplier<OptionalLong> lastHeartbeatAgeMsSupplier) {
        this.heartbeatStaleMs = heartbeatStaleMs;
        this.lastHeartbeatAgeMsSupplier = lastHeartbeatAgeMsSupplier;
    }

    public void onPartitionsAssigned() {
        assigned.set(true);
    }

    public void onPartitionsRevoked() {
        assigned.set(false);
    }

    @Override
    public Health health() {
        if (!assigned.get()) {
            return Health.outOfService().withDetail("reason", "no partitions assigned").build();
        }
        OptionalLong ageMs = lastHeartbeatAgeMsSupplier.get();
        if (ageMs.isEmpty()) {
            return Health.outOfService().withDetail("reason", "no heartbeat yet").build();
        }
        long age = ageMs.getAsLong();
        if (age > heartbeatStaleMs) {
            return Health.outOfService().withDetail("heartbeatStaleMs", age).build();
        }
        return Health.up().withDetail("heartbeatAgeMs", age).build();
    }
}
```

- [ ] **Step 2.2: Run the tests to confirm they pass**

Run: `./gradlew :platform:lib-java:test --tests com.canary.platform.lib.KafkaConsumerHealthIndicatorTest`
Expected: PASS — all 5 tests green.

- [ ] **Step 2.3: Commit**

```bash
git add platform/lib-java/src/main/java/com/canary/platform/lib/KafkaConsumerHealthIndicator.java \
        platform/lib-java/src/test/java/com/canary/platform/lib/KafkaConsumerHealthIndicatorTest.java
git commit -m "feat(lib-java): heartbeat-based Kafka readiness state machine"
```

---

## Task 3: Java lib — wire indicator + app event listeners + heartbeat supplier in autoconfig

**Files:**
- Modify: `platform/lib-java/src/main/java/com/canary/platform/lib/autoconfigure/XCanaryAutoConfiguration.java`

- [ ] **Step 3.1: Replace the `kafkaConsumerHealthIndicator` bean and add three new beans**

Replace lines 89-93 (the existing `kafkaConsumerHealthIndicator` bean) with the block below, and also add the three new beans (heartbeat supplier + two `ApplicationListener` beans). The Spring placeholder `${canary.kafka-heartbeat-stale-ms:${canary.kafka-health-timeout-ms:15000}}` reads the new property first, falls back to the old name as a deprecated alias, then to `15000`.

```java
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

@Bean
public ApplicationListener<ConsumerPartitionsAssignedEvent> kafkaConsumerPartitionsAssignedListener(
        KafkaConsumerHealthIndicator indicator) {
    return event -> indicator.onPartitionsAssigned();
}

@Bean
public ApplicationListener<ConsumerPartitionsRevokedEvent> kafkaConsumerPartitionsRevokedListener(
        KafkaConsumerHealthIndicator indicator) {
    return event -> indicator.onPartitionsRevoked();
}
```

- [ ] **Step 3.2: Add the new imports near the top of the file**

Add to the import block (alphabetical order with existing imports):

```java
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.springframework.context.ApplicationListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.event.ConsumerPartitionsAssignedEvent;
import org.springframework.kafka.event.ConsumerPartitionsRevokedEvent;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.OptionalLong;
import java.util.function.Supplier;
```

- [ ] **Step 3.3: Build the lib to confirm it compiles**

Run: `./gradlew :platform:lib-java:compileJava`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 3.4: Run the lib's tests**

Run: `./gradlew :platform:lib-java:test`
Expected: BUILD SUCCESSFUL — all existing tests still pass plus the 5 new indicator tests.

- [ ] **Step 3.5: Commit**

```bash
git add platform/lib-java/src/main/java/com/canary/platform/lib/autoconfigure/XCanaryAutoConfiguration.java
git commit -m "feat(lib-java): wire heartbeat-based readiness via app events + kafka-clients metric"
```

---

## Task 4: Java services — drop `recordPoll` from listeners (audit, payment, inventory)

The new indicator gets its data from app events + metrics. Listeners no longer need to inject the indicator or call any method on it.

**Files:**
- Modify: `services/audit-service/src/main/java/com/canary/audit/kafka/AuditKafkaListener.java`
- Modify: `services/payment-service/src/main/java/com/canary/payment/kafka/PaymentKafkaListener.java`
- Modify: `services/inventory-service/src/main/java/com/canary/inventory/kafka/InventoryKafkaListener.java`
- Modify: `services/audit-service/src/test/java/com/canary/audit/kafka/AuditKafkaListenerGatingTest.java`
- Modify: `services/payment-service/src/test/java/com/canary/payment/kafka/PaymentKafkaListenerGatingTest.java`
- Modify: `services/inventory-service/src/test/java/com/canary/inventory/kafka/InventoryKafkaListenerGatingTest.java`

- [ ] **Step 4.1: For each of the three listeners, remove the indicator field, constructor parameter, and the `health.recordPoll()` line**

For `PaymentKafkaListener.java` (the other two have the same shape):

Before:
```java
private final ConsumedEventStore store;
private final XCanaryConsumeFilter filter;
private final KafkaConsumerHealthIndicator health;

public PaymentKafkaListener(ConsumedEventStore store,
                            XCanaryConsumeFilter filter,
                            KafkaConsumerHealthIndicator health) {
    this.store = store;
    this.filter = filter;
    this.health = health;
}

@KafkaListener(...)
public void onMessage(ConsumerRecord<String, String> record) {
    health.recordPoll();
    if (!filter.shouldProcess(record.headers())) {
        return;
    }
    ...
}
```

After:
```java
private final ConsumedEventStore store;
private final XCanaryConsumeFilter filter;

public PaymentKafkaListener(ConsumedEventStore store,
                            XCanaryConsumeFilter filter) {
    this.store = store;
    this.filter = filter;
}

@KafkaListener(...)
public void onMessage(ConsumerRecord<String, String> record) {
    if (!filter.shouldProcess(record.headers())) {
        return;
    }
    ...
}
```

Also remove the now-unused `import com.canary.platform.lib.KafkaConsumerHealthIndicator;` line.

Apply the equivalent edits to `AuditKafkaListener.java` and `InventoryKafkaListener.java`.

- [ ] **Step 4.2: Update each gating test to drop the indicator from the listener constructor**

For `PaymentKafkaListenerGatingTest.java` (other two are equivalent):

Before:
```java
PaymentKafkaListener listener = new PaymentKafkaListener(store, filter, health);
```

After:
```java
PaymentKafkaListener listener = new PaymentKafkaListener(store, filter);
```

Also drop any `KafkaConsumerHealthIndicator health = new KafkaConsumerHealthIndicator(...)` setup line and its import.

- [ ] **Step 4.3: Verify any test that asserted `recordPoll` was called is removed**

Run: `grep -n "recordPoll" services/{audit,payment,inventory}-service/src/test/java -r`
Expected: no output. If any line remains (an old "filterRejectionShortCircuits asserts pollFlag" assertion), delete that assertion (the order-of-operations concern is no longer a concern; the listener no longer calls the indicator).

- [ ] **Step 4.4: Build and test all three Java services**

Run: `./gradlew :services:audit-service:test :services:payment-service:test :services:inventory-service:test`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 4.5: Commit**

```bash
git add services/audit-service/src services/payment-service/src services/inventory-service/src
git commit -m "refactor(java services): drop KafkaConsumerHealthIndicator from listeners"
```

---

## Task 5: Node lib — failing tests for new API

**Files:**
- Modify: `platform/lib-node/src/__tests__/kafka-consumer-health.test.ts`

- [ ] **Step 5.1: Replace the test file**

```ts
import { describe, expect, it } from "vitest";
import { createKafkaHealthState } from "../kafka-consumer-health.js";

describe("kafka-consumer-health", () => {
  it("not assigned → unhealthy", () => {
    const s = createKafkaHealthState(15_000);
    expect(s.isHealthy()).toBe(false);
    expect(s.report().ok).toBe(false);
    expect(s.report().reason).toMatch(/no partitions assigned/i);
  });

  it("assigned but no heartbeat → unhealthy", () => {
    const s = createKafkaHealthState(15_000);
    s.markAssigned();
    expect(s.isHealthy()).toBe(false);
    expect(s.report().reason).toMatch(/no heartbeat/i);
  });

  it("assigned + fresh heartbeat → healthy", () => {
    const s = createKafkaHealthState(15_000);
    s.markAssigned();
    s.recordHeartbeat();
    expect(s.isHealthy()).toBe(true);
    expect(s.report().ok).toBe(true);
  });

  it("assigned + stale heartbeat → unhealthy", async () => {
    const s = createKafkaHealthState(50);
    s.markAssigned();
    s.recordHeartbeat();
    await new Promise((r) => setTimeout(r, 100));
    expect(s.isHealthy()).toBe(false);
    expect(s.report().reason).toMatch(/stale/i);
  });

  it("revoked after assigned → unhealthy", () => {
    const s = createKafkaHealthState(15_000);
    s.markAssigned();
    s.recordHeartbeat();
    expect(s.isHealthy()).toBe(true);
    s.markRevoked();
    expect(s.isHealthy()).toBe(false);
  });
});
```

- [ ] **Step 5.2: Run tests to confirm they fail**

Run: `cd platform/lib-node && npm test -- kafka-consumer-health.test.ts`
Expected: FAIL — `markAssigned` / `recordHeartbeat` / `markRevoked` do not exist on `KafkaHealthState`.

---

## Task 6: Node lib — implement new API

**Files:**
- Modify: `platform/lib-node/src/kafka-consumer-health.ts`

- [ ] **Step 6.1: Replace the implementation**

```ts
export interface KafkaHealthReport {
  ok: boolean;
  reason?: string;
  ageMs?: number;
}

export interface KafkaHealthState {
  markAssigned(): void;
  markRevoked(): void;
  recordHeartbeat(): void;
  isHealthy(): boolean;
  report(): KafkaHealthReport;
}

export function createKafkaHealthState(heartbeatStaleMs: number = 15_000): KafkaHealthState {
  let assigned = false;
  let lastHeartbeatMs = 0;
  return {
    markAssigned() {
      assigned = true;
    },
    markRevoked() {
      assigned = false;
    },
    recordHeartbeat() {
      lastHeartbeatMs = Date.now();
    },
    isHealthy() {
      if (!assigned) return false;
      if (lastHeartbeatMs === 0) return false;
      return Date.now() - lastHeartbeatMs <= heartbeatStaleMs;
    },
    report() {
      if (!assigned) return { ok: false, reason: "no partitions assigned" };
      if (lastHeartbeatMs === 0) return { ok: false, reason: "no heartbeat yet" };
      const ageMs = Date.now() - lastHeartbeatMs;
      if (ageMs > heartbeatStaleMs) {
        return { ok: false, reason: `stale ${Math.floor(ageMs / 1000)}s`, ageMs };
      }
      return { ok: true, ageMs };
    },
  };
}
```

- [ ] **Step 6.2: Run tests to confirm they pass**

Run: `cd platform/lib-node && npm test -- kafka-consumer-health.test.ts`
Expected: PASS — all 5 tests green.

- [ ] **Step 6.3: Build lib-node**

Run: `cd platform/lib-node && npm run build`
Expected: success.

- [ ] **Step 6.4: Commit**

```bash
git add platform/lib-node/src/kafka-consumer-health.ts \
        platform/lib-node/src/__tests__/kafka-consumer-health.test.ts \
        platform/lib-node/dist/
git commit -m "feat(lib-node): heartbeat-based Kafka readiness state machine"
```

---

## Task 7: Node service — order-service kafka.ts wiring

**Files:**
- Modify: `services/order-service/src/kafka.ts`

- [ ] **Step 7.1: Replace the `recordPoll` call inside `eachMessage` with kafkajs event subscriptions**

Two edits in this file. First, after the line `const c = kafka.consumer({ groupId });`, add the four event subscriptions. Second, delete the `health.recordPoll();` line inside `eachMessage`. Also rename the `kafkaHealthTimeoutMs` field on `KafkaSetupOptions` to `heartbeatStaleMs` and propagate the rename through the call site.

The relevant patches:

```ts
// In KafkaSetupOptions interface
- /** Override kafka health timeout in ms. Defaults to 30000. */
- kafkaHealthTimeoutMs?: number;
+ /** Override heartbeat-staleness threshold in ms. Defaults to 15000. */
+ heartbeatStaleMs?: number;
```

```ts
// In setupKafka()
- const health = createKafkaHealthState(opts.kafkaHealthTimeoutMs ?? 30000);
+ const health = createKafkaHealthState(opts.heartbeatStaleMs ?? 15000);
```

```ts
// After `const c = kafka.consumer({ groupId });`
const c = kafka.consumer({ groupId });
consumer = c;
c.on(c.events.GROUP_JOIN, () => health.markAssigned());
c.on(c.events.REBALANCING, () => health.markRevoked());
c.on(c.events.HEARTBEAT, () => health.recordHeartbeat());
c.on(c.events.DISCONNECT, () => health.markRevoked());
```

```ts
// Inside eachMessage — delete this line
- // recordPoll fires on every message BEFORE the filter check.
- health.recordPoll();
```

- [ ] **Step 7.2: Update the index.ts wiring to pass the renamed option**

Edit `services/order-service/src/index.ts`:

```ts
- kafkaHealthTimeoutMs: config.KAFKA_HEALTH_TIMEOUT_MS,
+ heartbeatStaleMs: config.KAFKA_HEARTBEAT_STALE_MS,
```

- [ ] **Step 7.3: (No commit yet — config.ts and tests change next.)**

---

## Task 8: Node service — order-service config.ts (rename + alias)

**Files:**
- Modify: `services/order-service/src/config.ts`

- [ ] **Step 8.1: Rename the field; honor old env var as deprecated alias**

Replace:
```ts
KAFKA_HEALTH_TIMEOUT_MS: number;
```
with:
```ts
KAFKA_HEARTBEAT_STALE_MS: number;
```

Replace:
```ts
KAFKA_HEALTH_TIMEOUT_MS: Number(env.KAFKA_HEALTH_TIMEOUT_MS ?? 30000),
```
with:
```ts
KAFKA_HEARTBEAT_STALE_MS: (() => {
  if (env.KAFKA_HEARTBEAT_STALE_MS !== undefined) {
    return Number(env.KAFKA_HEARTBEAT_STALE_MS);
  }
  if (env.KAFKA_HEALTH_TIMEOUT_MS !== undefined) {
    console.warn(
      "KAFKA_HEALTH_TIMEOUT_MS is deprecated; use KAFKA_HEARTBEAT_STALE_MS",
    );
    return Number(env.KAFKA_HEALTH_TIMEOUT_MS);
  }
  return 15000;
})(),
```

---

## Task 9: Node service — order-service test updates

**Files:**
- Modify: `services/order-service/src/__tests__/config.test.ts`
- Modify: `services/order-service/src/__tests__/kafka.test.ts`
- Modify: `services/order-service/src/__tests__/http.test.ts`

- [ ] **Step 9.1: Rewrite config.test.ts to cover the new field + alias**

```ts
import { describe, expect, it, vi } from "vitest";
import { loadConfig } from "../config.js";

describe("loadConfig", () => {
  it("defaults KAFKA_HEARTBEAT_STALE_MS to 15000", () => {
    const cfg = loadConfig({});
    expect(cfg.KAFKA_HEARTBEAT_STALE_MS).toBe(15000);
  });

  it("respects KAFKA_HEARTBEAT_STALE_MS override", () => {
    const cfg = loadConfig({ KAFKA_HEARTBEAT_STALE_MS: "5000" });
    expect(cfg.KAFKA_HEARTBEAT_STALE_MS).toBe(5000);
  });

  it("honors deprecated KAFKA_HEALTH_TIMEOUT_MS with warn", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const cfg = loadConfig({ KAFKA_HEALTH_TIMEOUT_MS: "7000" });
    expect(cfg.KAFKA_HEARTBEAT_STALE_MS).toBe(7000);
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("deprecated"));
    warn.mockRestore();
  });

  it("new var wins over deprecated alias", () => {
    const cfg = loadConfig({
      KAFKA_HEARTBEAT_STALE_MS: "1000",
      KAFKA_HEALTH_TIMEOUT_MS: "9999",
    });
    expect(cfg.KAFKA_HEARTBEAT_STALE_MS).toBe(1000);
  });
});
```

- [ ] **Step 9.2: Update kafka.test.ts — replace the recordPoll spy block**

Locate the test `"recordPoll is called for each message regardless of filter result"` (around line 261) and replace it with a heartbeat-event-driven test:

```ts
it("HEARTBEAT events advance health.recordHeartbeat", async () => {
  process.env.VERSION = "canary";

  const kafkaHandle = await setupKafka({
    brokers: ["localhost:9092"],
    consumersEnabled: true,
    producerEnabled: false,
    presenceWatcherEnabled: false,
  });

  const recordHeartbeatSpy = vi.spyOn(kafkaHandle.health, "recordHeartbeat");
  const markAssignedSpy = vi.spyOn(kafkaHandle.health, "markAssigned");
  const markRevokedSpy = vi.spyOn(kafkaHandle.health, "markRevoked");

  // The captured consumer mock should expose the .on() registrations made by setupKafka.
  // Drive each registered handler manually via the existing mock harness.
  const consumer = kafkaHandle.consumer!;
  // kafkajs mock exposes handlers via its events emitter; trigger them directly.
  // (see existing test setup for how `captureEachMessage` works — same mechanism)
  triggerConsumerEvent(consumer, "consumer.heartbeat", {});
  triggerConsumerEvent(consumer, "consumer.group_join", {});
  triggerConsumerEvent(consumer, "consumer.rebalancing", {});

  expect(recordHeartbeatSpy).toHaveBeenCalledTimes(1);
  expect(markAssignedSpy).toHaveBeenCalledTimes(1);
  expect(markRevokedSpy).toHaveBeenCalledTimes(1);
});
```

If the kafkajs mock used in tests does not provide a `triggerConsumerEvent` helper, add one to the existing test mock setup file (typically near `captureEachMessage`). The implementation detail: kafkajs's `.on(eventName, fn)` stores the handler; the mock should expose a way to call those handlers directly.

If exposing the mock is too invasive, fall back to a unit-style test that bypasses `setupKafka` and asserts the wiring at the `KafkaHealthState` level only — the lib-node tests in Task 5 already cover the state machine.

- [ ] **Step 9.3: Update http.test.ts — replace stale-health setup**

Locate the two stale-health test blocks (around lines 73 and 86) and replace `createKafkaHealthState(1) + staleHealth.recordPoll()` with `createKafkaHealthState(1) + staleHealth.markAssigned() + staleHealth.recordHeartbeat()`.

Before:
```ts
const staleHealth = createKafkaHealthState(1);
staleHealth.recordPoll();
await new Promise((r) => setTimeout(r, 5));
```

After:
```ts
const staleHealth = createKafkaHealthState(1);
staleHealth.markAssigned();
staleHealth.recordHeartbeat();
await new Promise((r) => setTimeout(r, 5));
```

Apply the same edit to both occurrences in this file.

- [ ] **Step 9.4: Run order-service tests**

Run: `cd services/order-service && npm test`
Expected: PASS — all green.

- [ ] **Step 9.5: Commit**

```bash
git add services/order-service/src
git commit -m "feat(order-service): wire heartbeat-based Kafka readiness"
```

---

## Task 10: Node service — notification-service (mirror of Tasks 7–9)

**Files:**
- Modify: `services/notification-service/src/kafka.ts`
- Modify: `services/notification-service/src/index.ts`
- Modify: `services/notification-service/src/config.ts`
- Modify: `services/notification-service/src/__tests__/config.test.ts`
- Modify: `services/notification-service/src/__tests__/kafka.test.ts`
- Modify: `services/notification-service/src/__tests__/http.test.ts`

- [ ] **Step 10.1: Apply the same edits as Tasks 7, 8, and 9 to notification-service**

The notification-service `kafka.ts` has identical structure (the same `recordPoll` call, the same `kafkaHealthTimeoutMs` option, the same client structure) — apply the matching edits verbatim.

The notification-service tests have the same shape as order-service:
- `config.test.ts` (lines 14, 17–18): use the rewrite from Step 9.1
- `kafka.test.ts` (line 261 `recordPoll` block): use the rewrite from Step 9.2
- `http.test.ts` (lines 73, 86 stale-health setup): apply Step 9.3 edits

- [ ] **Step 10.2: Run notification-service tests**

Run: `cd services/notification-service && npm test`
Expected: PASS — all green.

- [ ] **Step 10.3: Commit**

```bash
git add services/notification-service/src
git commit -m "feat(notification-service): wire heartbeat-based Kafka readiness"
```

---

## Task 11: Tooling — Makefile, deploy.sh, pre-warm.sh, canary-overlay.yaml

**Files:**
- Modify: `Makefile`
- Modify: `deploy/services/deploy.sh`
- Modify: `deploy/services/pre-warm.sh`
- Modify: `deploy/helm/values/canary-overlay.yaml`

- [ ] **Step 11.1: Soften the Makefile `pre-warm` description**

In `Makefile` line 59, replace:
```makefile
pre-warm: ## Pre-warm Kafka topics with baseline orders (run BEFORE first canary on a cold cluster)
```
with:
```makefile
pre-warm: ## Send 3 baseline orders (optional; useful before e2e suites that measure consumer lag)
```

- [ ] **Step 11.2: Drop the post-deploy reminder in deploy.sh**

In `deploy/services/deploy.sh`, delete lines 41–44 (the `echo` statements about "run 'make pre-warm' before any 'make canary-deploy'"). Keep the `==> deploy-services complete` line.

- [ ] **Step 11.3: Reframe pre-warm.sh header comments**

In `deploy/services/pre-warm.sh` lines 6 and 16, replace references to `recordPoll`-as-prerequisite with the new framing. Specifically:

Replace any text describing `pre-warm` as REQUIRED to avoid the cold-start deadlock with: "Optional: seeds consumer offsets so e2e suites can measure lag from a known baseline (lag=0). With heartbeat-based readiness, canary deploys no longer require this."

- [ ] **Step 11.4: Update canary-overlay.yaml comment**

In `deploy/helm/values/canary-overlay.yaml`, replace the comment block at lines 18–22 (the one mentioning `recordPoll fired within canary.kafka-health-timeout-ms`) with:

```yaml
  # Canary-only readiness gating: kafkaConsumer indicator must report UP
  # (i.e. consumer joined + heartbeat fresh within
  # canary.kafka-heartbeat-stale-ms; default 15s). When canary's consumer
  # heartbeat thread freezes (SIGSTOP, panic, lost group membership),
  # readiness flips to 503 → kubelet drops the pod from EndpointSlice →
  # stable's pod-watch sees Ready=False → stable picks up the next
  # flagged event. Stable does NOT have this gating — see application.yml.
  MANAGEMENT_ENDPOINT_HEALTH_GROUP_READINESS_INCLUDE: "readinessState,kafkaConsumer"
```

- [ ] **Step 11.5: Commit**

```bash
git add Makefile deploy/services/deploy.sh deploy/services/pre-warm.sh deploy/helm/values/canary-overlay.yaml
git commit -m "chore(tooling): de-emphasize pre-warm; update canary-overlay comment"
```

---

## Task 12: Documentation updates

**Files:**
- Modify: `README.md`
- Modify: `docs/operations.md`
- Modify: `docs/canary-mechanics.md`
- Modify: `docs/development.md`
- Modify: `docs/history.md`

- [ ] **Step 12.1: README.md — drop the cold-cluster warning**

Find line 39:
```
make pre-warm                                   # ⚠ REQUIRED before first canary on a cold cluster
```
Replace with:
```
make pre-warm                                   # optional: seeds consumer offsets to lag=0 for e2e suites
```

In the "Build + deploy all services" row of the table (around line 97), drop `&& make pre-warm` from the command (it's no longer required for correctness).

In the "Known issues" / quirks section (around lines 124–127), remove the `Cold-cluster pre-warm` bullet entirely (the issue is fixed at the root cause; surface this in `docs/history.md` instead).

- [ ] **Step 12.2: docs/operations.md — rewrite cold-cluster section**

Replace the "Cold-cluster pre-warm" section (lines 29–48) with:

```markdown
### Cold-cluster pre-warm (optional)

`make pre-warm` sends 3 baseline (non-canary) orders, seeding every
consumer group's offset. **No longer required** — heartbeat-based
readiness lets canary pods reach Ready immediately on a cold cluster.
Useful before running e2e suites (K1–K5) that assert lag-related
behavior, since it gives every consumer a known starting offset.

Tunable knobs (env vars):

- `PRE_WARM_COUNT` (default 3)
- `PRE_WARM_DELAY_MS` (default 1000)
- `PRE_WARM_URL` (default `http://localhost:8080/api/orders`)
```

In the "Cold-cluster bring-up" code block (line 24), drop the `⚠` annotation on `make pre-warm`. Keep the line itself with a `# optional` comment.

In the "Troubleshooting" section (lines 197–200), delete the `"helm install --wait timed out" on a fresh cluster` ⇒ "you skipped pre-warm" entry. The new probe doesn't have this failure mode.

- [ ] **Step 12.3: docs/canary-mechanics.md — update probe explanation**

Replace lines 156–207 (the cold-start mechanics section) with text matching the new probe. Specifically:

- Replace the code sample showing `health.recordPoll()` (around line 158) with the new event-driven snippet:
  ```ts
  // kafkajs
  c.on(c.events.GROUP_JOIN, () => health.markAssigned());
  c.on(c.events.HEARTBEAT,  () => health.recordHeartbeat());
  c.on(c.events.REBALANCING, () => health.markRevoked());
  c.on(c.events.DISCONNECT, () => health.markRevoked());
  ```
- Replace the "recordPoll stops firing" mechanism description (line 182) with: "the canary's heartbeat thread is frozen by SIGSTOP, so `last-heartbeat-seconds-ago` (Java) / `consumer.events.HEARTBEAT` (Node) goes stale beyond `canary.kafka-heartbeat-stale-ms` (default 15s)."
- Replace "the other half of the cold-start fix is `make pre-warm`" (lines 206–207) with: "Cold-start is no longer a problem — `make pre-warm` is documented as an optional e2e helper in [operations.md](operations.md#cold-cluster-pre-warm-optional)."

- [ ] **Step 12.4: docs/development.md — drop "REQUIRED" annotation**

In the make-target table (line 65):
Before:
```
| `make pre-warm` | Send 3 baseline orders (REQUIRED before first canary on a cold cluster) |
```
After:
```
| `make pre-warm` | Send 3 baseline orders (optional; useful before e2e suites that measure consumer lag) |
```

In the "Known Spring Boot 4 quirks" section (lines 184–189), the paragraph mentioning `recordPoll` and pre-warm should be reworked: the `auto-offset-reset=earliest` + heartbeat-based readiness combo means the cold-start deadlock is structurally fixed. Replace the paragraph with: "Canary readiness uses consumer heartbeat freshness (`last-heartbeat-seconds-ago` metric), not message receipt. A brand-new canary pod becomes Ready as soon as its consumer joins the group and emits a heartbeat — typically <5s after pod start, even on a cluster with zero traffic."

- [ ] **Step 12.5: docs/history.md — append the post-merge fix**

In `docs/history.md`, find the post-merge fixes section ("Post-merge fixes (cluster verification surfaced 5 issues)" around line 180) and append a new entry as **Item 6**:

```markdown
- **Item 6 — Cold-cluster pre-warm fixed at root cause.** Item 1's
  `make pre-warm` was a workaround. Replaced the "first poll received"
  Kafka readiness signal with "consumer joined + heartbeat fresh"
  (Java: `last-heartbeat-seconds-ago` metric; Node: kafkajs
  `consumer.events.HEARTBEAT`). Threshold default 15s (was 30s for the
  poll-receipt timeout). K5 detection is now ~31s end-to-end (was ~46s).
  `make pre-warm` is kept as an optional e2e helper. Stable readiness
  unchanged. Old env-var/property names accepted as deprecated aliases.
  Spec: `docs/superpowers/specs/2026-05-10-canary-cold-cluster-prewarm-fix-design.md`.
```

- [ ] **Step 12.6: Commit**

```bash
git add README.md docs/operations.md docs/canary-mechanics.md docs/development.md docs/history.md
git commit -m "docs: heartbeat-based readiness; pre-warm reframed as optional"
```

---

## Task 13: New e2e scenario K6 — cold-cluster bring-up without pre-warm

**Files:**
- Create: `tests/e2e/k6-cold-cluster-bringup.test.ts`

- [ ] **Step 13.1: Look at the existing K5 test file for shape, helpers, and env conventions**

Read `tests/e2e/k5-canary-kafka-unhealthy.test.ts` (lines 1–67) and `tests/e2e/helpers/cluster.ts` (or wherever `ensureCleanBaseline` is defined) to confirm the available helpers.

- [ ] **Step 13.2: Write the K6 test**

```ts
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { execa } from "execa";
import { deployCanary, rollback } from "./helpers/canary.js";
import { findPodByLabel } from "./helpers/pod-port-forward.js";

// K6 is gated behind RUN_COLD_CLUSTER_TESTS=true because it tears down and
// re-deploys all 5 services (~4 min). Default e2e suite skips it; manual
// cluster verification opts in.
const RUN = process.env.RUN_COLD_CLUSTER_TESTS === "true";

(RUN ? describe : describe.skip)(
  "K6 — cold-cluster bring-up succeeds without make pre-warm",
  () => {
    afterAll(async () => {
      try { await rollback("audit-service"); } catch {}
    });

    it("make undeploy-services + make deploy-services + canary-deploy without pre-warm", async () => {
      const repoRoot = new URL("../..", import.meta.url).pathname;

      // Step A: tear down to a known cold-cluster state
      await execa("make", ["undeploy-services"], { cwd: repoRoot, stdio: "inherit", timeout: 180_000 });

      // Step B: redeploy services with the default helm --wait (3min). Asserts
      // helm wait does NOT time out — i.e. stable pods become Ready inside the
      // helm budget without any traffic in the cluster.
      const deployResult = await execa("make", ["deploy-services"], {
        cwd: repoRoot,
        stdio: "inherit",
        timeout: 240_000,
      });
      expect(deployResult.exitCode).toBe(0);

      // Step C: deploy a canary WITHOUT running pre-warm. Asserts canary Helm
      // install --wait completes — the headline assertion of K6.
      await deployCanary("audit-service", "dev");

      // Step D: confirm canary readiness is 200 within 30s of pod creation.
      const canaryPod = await findPodByLabel("services", "app=audit-service,version=canary");
      const start = Date.now();
      let ready = false;
      while (Date.now() - start < 30_000) {
        const probe = await execa(
          "kubectl",
          [
            "-n", "services",
            "exec", canaryPod, "--",
            "curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
            "localhost:8081/actuator/health/readiness",
          ],
          { reject: false },
        );
        if (probe.stdout.trim() === "200") {
          ready = true;
          break;
        }
        await new Promise((r) => setTimeout(r, 2_000));
      }
      expect(ready).toBe(true);
    }, 480_000);
  },
);
```

- [ ] **Step 13.3: Add a npm script for the cold-cluster suite (optional but helpful)**

In `tests/e2e/package.json` `scripts`, add:
```json
"e2e:cold": "RUN_COLD_CLUSTER_TESTS=true vitest run k6-cold-cluster-bringup.test.ts"
```

- [ ] **Step 13.4: Sanity-build the e2e package (TypeScript only, no cluster needed)**

Run: `cd tests/e2e && npx tsc --noEmit`
Expected: no type errors.

- [ ] **Step 13.5: Commit**

```bash
git add tests/e2e/k6-cold-cluster-bringup.test.ts tests/e2e/package.json
git commit -m "test(e2e): add K6 cold-cluster bring-up scenario (gated)"
```

---

## Task 14: Final verification

- [ ] **Step 14.1: Build everything (Java + Node)**

Run: `make build-services`
Expected: BUILD SUCCESSFUL across all 5 services + libs.

- [ ] **Step 14.2: Run all unit tests**

Run: `./gradlew test && (cd platform/lib-node && npm test) && (cd services/order-service && npm test) && (cd services/notification-service && npm test)`
Expected: all green.

- [ ] **Step 14.3: Confirm zero references to old API names remain in production code**

Run: `grep -rn "recordPoll\|kafkaHealthTimeoutMs" platform/lib-java/src/main platform/lib-node/src services/*/src/main services/*/src/index.ts services/*/src/kafka.ts services/*/src/http.ts services/*/src/config.ts 2>/dev/null`
Expected: no output. (Test files may still reference deprecated names where they're testing the alias; that's fine.)

- [ ] **Step 14.4: Confirm zero references to old env-var name in deploy/Helm**

Run: `grep -rn "KAFKA_HEALTH_TIMEOUT_MS\|kafka-health-timeout-ms" deploy/`
Expected: no output. (The Java app still accepts the old property name as alias, but no deploy file should be using it.)

- [ ] **Step 14.5: Final commit (if any cleanup)**

```bash
git status
# If files modified: git add ... && git commit -m "..."
# Otherwise nothing to do.
```

- [ ] **Step 14.6: Merge back to main**

Per the user's saved plan-execution preference (isolated worktree + subagent-driven + merge-back-to-main), the executor merges the worktree branch back to `main` once all tasks are complete and verification passes.

```bash
# From the worktree:
git log --oneline main..HEAD   # confirm commit list
# From main:
git merge --no-ff <worktree-branch> -m "Merge: cold-cluster pre-warm fix (heartbeat-based readiness)"
```

---

## Spec coverage check

| Spec section | Covered by |
|---|---|
| Java `KafkaConsumerHealthIndicator` rewrite | Tasks 1, 2 |
| Java `XCanaryAutoConfiguration` wiring (app events + heartbeat supplier + property alias) | Task 3 |
| Java services (audit/payment/inventory) drop `recordPoll` | Task 4 |
| Node lib new API + tests | Tasks 5, 6 |
| Node services kafka.ts wiring + recordPoll removal | Tasks 7, 10 |
| Node services config.ts rename + alias | Tasks 8, 10 |
| Node services tests (config/kafka/http) | Tasks 9, 10 |
| Helm canary-overlay comment update | Task 11 |
| Makefile pre-warm description softening | Task 11 |
| deploy.sh post-deploy reminder removal | Task 11 |
| pre-warm.sh header reframing | Task 11 |
| README.md / operations.md / canary-mechanics.md / development.md / history.md | Task 12 |
| K6 e2e scenario | Task 13 |
| Default `15000` for both libs | Tasks 2, 6 |
| Old env-var/property as deprecated alias | Tasks 3, 8, 10 |
| Stable readiness unchanged | (no task — by exclusion; not in any task) |
