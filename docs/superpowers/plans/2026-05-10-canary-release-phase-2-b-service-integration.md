# Phase 2.b — Service Integration + Phase 2 e2e Scenarios (K1–K5) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the Phase 2.a lib abstractions (per-subset consumer-group, header filter, presence watcher, health indicator) into all 5 services, flip `KAFKA_CONSUMERS_ENABLED=true` on canary, and prove the Phase 2 rules end-to-end with 5 new scenarios K1–K5.

**Architecture:** Each Java service's `@KafkaListener` resolves its `groupId` via SpEL on the autoconfigured `XCanaryConsumerGroupIdResolver` bean, then on each message: records a poll timestamp (`KafkaConsumerHealthIndicator.recordPoll()`), short-circuits via `XCanaryConsumeFilter.shouldProcess(headers)`, and wraps the handler with `XCanaryConsumeContext.runWith(headers, ...)`. Each Node service's `kafka.ts` does the equivalent using `resolveConsumerGroupId` + `shouldProcess` + `runWithCanaryFromHeaders` + `createKafkaHealthState().recordPoll()`, with the presence watcher started at boot and the consumer-health state surfaced through `/health` (returns 503 when stale). Java services add the `kafkaConsumer` health indicator to the Spring Actuator `readiness` probe group so the kubelet drops the canary pod from endpoints when its consumer stalls. Canary-overlay flips `KAFKA_CONSUMERS_ENABLED` from `"false"` → `"true"`. New e2e scenarios K1–K5 send HTTP traffic with/without `x-canary` through the existing edge gateway (`/api/orders` → order-service); for verification they query each subset directly via `kubectl port-forward pod/<name>` to read that pod's `/internal/consumed-events`, since the edge gateway only routes order traffic and Istio subset-by-header is in-mesh-only.

**Tech Stack:** Java 25 + Spring Boot 4 + spring-kafka, Node 25 + kafkajs, Helm, Istio mesh routing, kubectl port-forward (for test introspection), Vitest e2e.

---

## Phase 2.a deliverables already on `main` (do NOT reimplement)

These are the building blocks 2.b consumes. Their public API is fixed.

**lib-java (`platform/lib-java/src/main/java/com/canary/platform/lib/`)**

| Class | Constructor | Key methods |
|---|---|---|
| `XCanaryConsumerGroupIdResolver` | `(String version)` | `String resolve(String baseGroupId)` |
| `XCanaryConsumeFilter` | `(String ownVersion, BooleanSupplier canaryReady)` | `boolean shouldProcess(Headers)`, static `boolean isCanaryFlagged(Headers)` |
| `XCanaryConsumeContext` | static utility | `static void runWith(Headers, Runnable)` |
| `XCanaryPresenceWatcher` | `(String namespace, String serviceName)` | `void start()`, `boolean isCanaryReady()`, `void close()` (AutoCloseable) |
| `KafkaConsumerHealthIndicator` | `(long timeoutMs)` | `void recordPoll()`, `Health health()` (Spring Actuator) |

**`XCanaryAutoConfiguration` (already on classpath):** auto-wires all 5 of those beans. SpEL bean names: `xCanaryConsumerGroupIdResolver`, `xCanaryConsumeFilter`, `kafkaConsumerHealthIndicator`, `xCanaryPresenceWatcher` (optional, conditional on `canary.presence-watcher.enabled=true`). The version is read from `${canary.version:${VERSION:stable}}`; the service name from `${canary.service-name:${SERVICE_NAME:unknown}}`; namespace from `${canary.namespace:${POD_NAMESPACE:services}}`.

**lib-node (`platform/lib-node/src/`, exported from `index.ts`)**

| Module | Exports |
|---|---|
| `x-canary-consumer-group.ts` | `resolveConsumerGroupId(baseGroupId: string): string` |
| `x-canary-consume-filter.ts` | `shouldProcess(headers, ownVersion, isCanaryReady): boolean`, `isCanaryFlagged(headers): boolean` |
| `x-canary-consume-context.ts` | `runWithCanaryFromHeaders(headers, async handler): Promise<T>` |
| `x-canary-presence-watcher.ts` | `class XCanaryPresenceWatcher(namespace, serviceName, kc?)` with `async start(): Promise<void>`, `close(): void` (sync), `isCanaryReady(): boolean` |
| `kafka-consumer-health.ts` | `createKafkaHealthState(timeoutMs?)` → `{recordPoll(), isHealthy(), report()}` |

`resolveConsumerGroupId` reads `process.env.VERSION` (default `"stable"`) and returns `<baseGroupId>-<version>`.

**Helm (`deploy/helm/service-chart/`)**

- `templates/role.yaml`: Role + RoleBinding granting `pods get/list/watch`, gated on `canaryWatch.enabled` (default `true`).
- `values.yaml` already declares `canaryWatch.enabled: true` and `KAFKA_CONSUMERS_ENABLED: "true"` (base default).
- `deploy/helm/values/canary-overlay.yaml` currently sets `KAFKA_CONSUMERS_ENABLED: "false"` — Task 6 flips to `"true"`.

---

## Locked design (settled in 2.a — do NOT relitigate)

1. Per-subset consumer groups: `<svc>-stable` and `<svc>-canary`. (e.g., `audit-service-stable` / `audit-service-canary`.)
2. Per-message filter: stable processes if `x-canary != "true"` OR `canaryReady == false`; canary processes only if `x-canary == "true"`.
3. K8s API Pod-watch (push-based) with `app=<svc>,version=canary` selector. Hot-path is O(1) atomic read.
4. Canary readiness gated on Kafka consumer health — Java via Actuator `kafkaConsumer` indicator in the `readiness` probe group; Node via `/health` returning 503 when the in-memory health state reports stale.
5. Brief duplicate-processing race window during canary becoming Ready / Kafka reconnecting is accepted; downstream handlers must be idempotent.

---

## File map

### Modified service files

```
services/audit-service/src/main/java/com/canary/audit/kafka/AuditKafkaListener.java   # MODIFY
services/audit-service/src/main/resources/application.yml                              # MODIFY (readiness group)
services/audit-service/src/test/java/com/canary/audit/kafka/AuditKafkaListenerGatingTest.java  # MODIFY (add canary tests)

services/payment-service/src/main/java/com/canary/payment/kafka/PaymentKafkaListener.java   # MODIFY
services/payment-service/src/main/resources/application.yml                                  # MODIFY
services/payment-service/src/test/java/com/canary/payment/kafka/PaymentKafkaListenerGatingTest.java  # MODIFY

services/inventory-service/src/main/java/com/canary/inventory/kafka/InventoryKafkaListener.java   # MODIFY
services/inventory-service/src/main/resources/application.yml                                       # MODIFY
services/inventory-service/src/test/java/com/canary/inventory/kafka/InventoryKafkaListenerGatingTest.java  # MODIFY

services/order-service/src/kafka.ts                                                    # MODIFY
services/order-service/src/http.ts                                                     # MODIFY (/health gate)
services/order-service/src/index.ts                                                    # MODIFY (start watcher)
services/order-service/src/__tests__/kafka.test.ts                                     # MODIFY (canary tests)

services/notification-service/src/kafka.ts                                             # MODIFY
services/notification-service/src/http.ts                                              # MODIFY (/health gate)
services/notification-service/src/index.ts                                             # MODIFY (start watcher)
services/notification-service/src/__tests__/kafka.test.ts                              # MODIFY
```

### Modified deployment files

```
deploy/helm/values/canary-overlay.yaml                                                 # MODIFY: flip to "true"
```

### New e2e scenarios + helpers

```
tests/e2e/helpers/pod-port-forward.ts                                                  # NEW (kubectl port-forward + signal helpers)
tests/e2e/helpers/consumed-events.ts                                                   # NEW (subset-aware query helper)
tests/e2e/k1-canary-flagged-event.test.ts                                              # NEW
tests/e2e/k2-canary-unflagged-event.test.ts                                            # NEW
tests/e2e/k3-no-canary-fallback.test.ts                                                # NEW
tests/e2e/k4-kafka-header-propagation.test.ts                                          # NEW
tests/e2e/k5-canary-kafka-unhealthy.test.ts                                            # NEW
```

### Misc

```
README.md                                                                              # MODIFY: add Plan 2.b section
```

---

## Conventions for every per-service task

- **One commit per task.** Commit message format mirrors existing log: `feat(<svc>): wire Phase 2.b canary consumer (groupId resolver + filter + health)`.
- **TDD for service tests:** write/extend the unit test FIRST, watch it fail, then change the listener/`kafka.ts`.
- **Build verify before commit:** Java services run `./gradlew :services:<svc>:test` (passes) and `./gradlew :services:<svc>:build` (passes). Node services run `pnpm -F @canary/<svc> build && pnpm -F @canary/<svc> test`.
- **Do NOT add new helper modules in services** — all required primitives are already exported by `lib-java` / `@canary/lib-node`.

---

### Task 1: Wire `audit-service` (Java)

**Files:**
- Modify: `services/audit-service/src/main/java/com/canary/audit/kafka/AuditKafkaListener.java`
- Modify: `services/audit-service/src/main/resources/application.yml`
- Modify: `services/audit-service/src/test/java/com/canary/audit/kafka/AuditKafkaListenerGatingTest.java`

- [ ] **Step 1: Extend the gating test to express the four-cell canary filter table**

Add these four tests to `AuditKafkaListenerGatingTest.java` (in addition to the existing three Conditional tests). They must run against a real instance of `AuditKafkaListener` but bypass actual Kafka — call `onMessage(record)` directly with constructed `ConsumerRecord` instances. Stub `XCanaryConsumeFilter` and `KafkaConsumerHealthIndicator` and `ConsumedEventStore` via the `TestStubs` configuration.

```java
import com.canary.platform.lib.KafkaConsumerHealthIndicator;
import com.canary.platform.lib.XCanaryConsumeFilter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

// Inside class body, additional tests:

@Test
void recordPollIsCalledOnEveryMessage() {
    runner.run(ctx -> {
        AuditKafkaListener listener = ctx.getBean(AuditKafkaListener.class);
        AtomicBoolean polled = ctx.getBean("pollFlag", AtomicBoolean.class);
        polled.set(false);
        listener.onMessage(record("orders.events", "k", "v", false));
        assertThat(polled.get()).isTrue();
    });
}

@Test
void filterRejectionShortCircuits() {
    runner.run(ctx -> {
        AuditKafkaListener listener = ctx.getBean(AuditKafkaListener.class);
        ConsumedEventStore store = ctx.getBean(ConsumedEventStore.class);
        AtomicBoolean shouldProcess = ctx.getBean("shouldProcessFlag", AtomicBoolean.class);
        shouldProcess.set(false);
        int before = store.all().size();
        listener.onMessage(record("orders.events", "k", "v", true));
        assertThat(store.all().size()).isEqualTo(before);
    });
}

@Test
void filterAcceptStoresEvent() {
    runner.run(ctx -> {
        AuditKafkaListener listener = ctx.getBean(AuditKafkaListener.class);
        ConsumedEventStore store = ctx.getBean(ConsumedEventStore.class);
        AtomicBoolean shouldProcess = ctx.getBean("shouldProcessFlag", AtomicBoolean.class);
        shouldProcess.set(true);
        int before = store.all().size();
        listener.onMessage(record("orders.events", "k1", "v1", true));
        assertThat(store.all().size()).isEqualTo(before + 1);
    });
}

@Test
void canaryHeaderIsPersistedToStoredEvent() {
    runner.run(ctx -> {
        AuditKafkaListener listener = ctx.getBean(AuditKafkaListener.class);
        AtomicBoolean shouldProcess = ctx.getBean("shouldProcessFlag", AtomicBoolean.class);
        shouldProcess.set(true);
        listener.onMessage(record("orders.events", "k2", "v2", true));
        ConsumedEventStore store = ctx.getBean(ConsumedEventStore.class);
        var last = store.all().get(store.all().size() - 1);
        assertThat(last.headers().get("x-canary")).isEqualTo("true");
    });
}

private static ConsumerRecord<String, String> record(String topic, String key, String value, boolean canary) {
    RecordHeaders headers = new RecordHeaders();
    if (canary) headers.add(new RecordHeader("x-canary", "true".getBytes(StandardCharsets.UTF_8)));
    return new ConsumerRecord<>(topic, 0, 0L, 0L,
            TimestampType.NO_TIMESTAMP_TYPE, -1, -1, key, value, headers, Optional.empty());
}
```

Note: the existing `runner` field has a stale Javadoc claiming the test only verifies @ConditionalOnProperty. After these additions that's no longer accurate. Replace the Javadoc with a single-line comment or delete it entirely:

```java
// No Kafka infrastructure is wired; onMessage is called directly.
```

Update `TestStubs` to wire the new collaborators:

```java
@Configuration
static class TestStubs {
    @Bean
    ConsumedEventStore consumedEventStore() {
        return new ConsumedEventStore();
    }

    @Bean
    AtomicBoolean shouldProcessFlag() {
        return new AtomicBoolean(true);
    }

    @Bean
    AtomicBoolean pollFlag() {
        return new AtomicBoolean(false);
    }

    @Bean
    XCanaryConsumeFilter xCanaryConsumeFilter(AtomicBoolean shouldProcessFlag) {
        return new XCanaryConsumeFilter("stable", () -> false) {
            @Override
            public boolean shouldProcess(org.apache.kafka.common.header.Headers headers) {
                return shouldProcessFlag.get();
            }
        };
    }

    @Bean
    KafkaConsumerHealthIndicator kafkaConsumerHealthIndicator(AtomicBoolean pollFlag) {
        return new KafkaConsumerHealthIndicator(30000) {
            @Override
            public void recordPoll() {
                pollFlag.set(true);
            }
        };
    }
}
```

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `./gradlew :services:audit-service:test --tests AuditKafkaListenerGatingTest`
Expected: 4 new tests fail (listener doesn't yet inject the new beans, doesn't call recordPoll, doesn't gate on filter, doesn't capture header into context).

- [ ] **Step 3: Modify `AuditKafkaListener` to wire the lib primitives**

Replace the entire file contents:

```java
package com.canary.audit.kafka;

import com.canary.audit.store.ConsumedEvent;
import com.canary.audit.store.ConsumedEventStore;
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
public class AuditKafkaListener {

    private final ConsumedEventStore store;
    private final XCanaryConsumeFilter filter;
    private final KafkaConsumerHealthIndicator health;

    public AuditKafkaListener(ConsumedEventStore store,
                              XCanaryConsumeFilter filter,
                              KafkaConsumerHealthIndicator health) {
        this.store = store;
        this.filter = filter;
        this.health = health;
    }

    @KafkaListener(
        topics = {"orders.events", "payments.events", "inventory.events", "notifications.events"},
        groupId = "#{xCanaryConsumerGroupIdResolver.resolve('audit-service')}"
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
```

- [ ] **Step 4: Add the `kafkaConsumer` health indicator to the readiness probe group**

Modify `services/audit-service/src/main/resources/application.yml`. Replace the `management` block with:

```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,info
  endpoint:
    health:
      probes:
        enabled: true
      show-details: never
      group:
        readiness:
          include: readinessState,kafkaConsumer
```

Why: Spring Actuator's `readiness` group by default contains only `readinessState`. Adding `kafkaConsumer` causes `/actuator/health/readiness` to return non-200 when the consumer's last poll exceeds `canary.kafka-health-timeout-ms` (default 30s), which fails the kubelet readiness probe.

- [ ] **Step 5: Run all audit-service tests; verify they pass**

Run: `./gradlew :services:audit-service:test`
Expected: All tests pass (existing 3 ConditionalOnProperty + 4 new canary tests).

- [ ] **Step 6: Build the service to verify nothing else broke**

Run: `./gradlew :services:audit-service:build`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 7: Commit**

```bash
git add services/audit-service/src/main/java/com/canary/audit/kafka/AuditKafkaListener.java \
        services/audit-service/src/main/resources/application.yml \
        services/audit-service/src/test/java/com/canary/audit/kafka/AuditKafkaListenerGatingTest.java
git commit -m "$(cat <<'EOF'
feat(audit): wire Phase 2.b canary consumer (group resolver + filter + health)

- Replace static groupId with SpEL on XCanaryConsumerGroupIdResolver bean
- Gate each message on XCanaryConsumeFilter.shouldProcess()
- Wrap handler with XCanaryConsumeContext.runWith() for header propagation
- Record poll on every message via KafkaConsumerHealthIndicator
- Include kafkaConsumer in actuator readiness probe group
EOF
)"
```

---

### Task 2: Wire `payment-service` (Java)

**Files:**
- Modify: `services/payment-service/src/main/java/com/canary/payment/kafka/PaymentKafkaListener.java`
- Modify: `services/payment-service/src/main/resources/application.yml`
- Modify: `services/payment-service/src/test/java/com/canary/payment/kafka/PaymentKafkaListenerGatingTest.java`

- [ ] **Step 1: Extend `PaymentKafkaListenerGatingTest.java` with the same four-cell canary tests**

Use the exact pattern from Task 1, Step 1, substituting `PaymentKafkaListener` for `AuditKafkaListener`. The test file already follows the same `ApplicationContextRunner` + `TestStubs` pattern.

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `./gradlew :services:payment-service:test --tests PaymentKafkaListenerGatingTest`
Expected: 4 new tests fail.

- [ ] **Step 3: Modify `PaymentKafkaListener` to wire the lib primitives**

Replace the entire file contents:

```java
package com.canary.payment.kafka;

import com.canary.payment.store.ConsumedEvent;
import com.canary.payment.store.ConsumedEventStore;
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
public class PaymentKafkaListener {

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

    @KafkaListener(
        topics = "orders.events",
        groupId = "#{xCanaryConsumerGroupIdResolver.resolve('payment-service')}"
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
```

- [ ] **Step 4: Update payment-service `application.yml` readiness group**

Same change as Task 1 Step 4 — replace the `management` block with the version that adds `readiness.include: readinessState,kafkaConsumer`.

- [ ] **Step 5: Run tests**

Run: `./gradlew :services:payment-service:test`
Expected: All pass.

- [ ] **Step 6: Build**

Run: `./gradlew :services:payment-service:build`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 7: Commit**

```bash
git add services/payment-service/src/main/java/com/canary/payment/kafka/PaymentKafkaListener.java \
        services/payment-service/src/main/resources/application.yml \
        services/payment-service/src/test/java/com/canary/payment/kafka/PaymentKafkaListenerGatingTest.java
git commit -m "feat(payment): wire Phase 2.b canary consumer (group resolver + filter + health)"
```

---

### Task 3: Wire `inventory-service` (Java)

**Files:**
- Modify: `services/inventory-service/src/main/java/com/canary/inventory/kafka/InventoryKafkaListener.java`
- Modify: `services/inventory-service/src/main/resources/application.yml`
- Modify: `services/inventory-service/src/test/java/com/canary/inventory/kafka/InventoryKafkaListenerGatingTest.java`

- [ ] **Step 1: Extend `InventoryKafkaListenerGatingTest.java`**

Same four-cell canary tests as Task 1, Step 1, substituting `InventoryKafkaListener`.

- [ ] **Step 2: Run the new tests to verify they fail**

Run: `./gradlew :services:inventory-service:test --tests InventoryKafkaListenerGatingTest`
Expected: 4 new tests fail.

- [ ] **Step 3: Modify `InventoryKafkaListener` — same pattern as Task 2 Step 3**

Replace contents (identical structure, only the package, class name, and groupId base name `'inventory-service'` differ):

```java
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
```

- [ ] **Step 4: Update inventory-service `application.yml` readiness group**

Same change as Task 1 Step 4.

- [ ] **Step 5: Run tests**

Run: `./gradlew :services:inventory-service:test`
Expected: All pass.

- [ ] **Step 6: Build**

Run: `./gradlew :services:inventory-service:build`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 7: Commit**

```bash
git add services/inventory-service/src/main/java/com/canary/inventory/kafka/InventoryKafkaListener.java \
        services/inventory-service/src/main/resources/application.yml \
        services/inventory-service/src/test/java/com/canary/inventory/kafka/InventoryKafkaListenerGatingTest.java
git commit -m "feat(inventory): wire Phase 2.b canary consumer (group resolver + filter + health)"
```

---

### Task 4: Wire `order-service` (Node)

**Files:**
- Modify: `services/order-service/src/kafka.ts`
- Modify: `services/order-service/src/http.ts`
- Modify: `services/order-service/src/index.ts`
- Modify: `services/order-service/src/__tests__/kafka.test.ts`

- [ ] **Step 1: Read the current `kafka.test.ts` to understand existing patterns**

Run: `cat services/order-service/src/__tests__/kafka.test.ts`

Use the existing test file's mocking style — likely it stubs `kafkajs` with a fake consumer/producer. Add the new tests in that style.

- [ ] **Step 2: Extend `kafka.test.ts` with the four canary cases + group-id resolution**

Append these tests (using vitest + the existing kafkajs mock pattern in the file):

```typescript
import { setupKafka } from "../kafka.js";
import { consumedEventStore } from "../store.js";

// Existing tests assumed; add:

describe("setupKafka — Phase 2.b canary integration", () => {
  beforeEach(() => {
    consumedEventStore.clear?.();
    process.env.VERSION = "stable";
  });

  it("uses resolveConsumerGroupId('order-service') for the group id", async () => {
    const created: { groupId?: string }[] = [];
    const fakeKafka = {
      consumer: (cfg: { groupId: string }) => {
        created.push(cfg);
        return {
          connect: async () => {},
          subscribe: async () => {},
          run: async () => {},
        };
      },
      producer: () => ({ connect: async () => {}, send: async () => {} }),
    };
    // Inject fakeKafka via test seam (existing test file already does this).
    // Expectation:
    //   process.env.VERSION = 'stable' → groupId = 'order-service-stable'
    //   process.env.VERSION = 'canary' → groupId = 'order-service-canary'
    // Adapt to the existing test seam in this file.
    process.env.VERSION = "canary";
    // ... call setupKafka via the existing seam ...
    expect(created[0].groupId).toBe("order-service-canary");
  });

  it("filter: stable + canaryReady=true skips x-canary messages", async () => {
    process.env.VERSION = "stable";
    // Drive eachMessage with x-canary=true and a presence watcher returning Ready=true.
    // Expect consumedEventStore not to record the message.
  });

  it("filter: canary processes x-canary messages", async () => {
    process.env.VERSION = "canary";
    // Drive eachMessage with x-canary=true.
    // Expect consumedEventStore to record exactly one event.
  });

  it("filter: canary skips messages without x-canary header", async () => {
    process.env.VERSION = "canary";
    // Drive eachMessage with no x-canary header.
    // Expect consumedEventStore not to record the message.
  });

  it("recordPoll is called for each message regardless of filter result", async () => {
    process.env.VERSION = "stable";
    // Spy on a kafkaHealthState injected via the test seam.
    // Drive 3 messages; assert recordPoll called 3 times.
  });

  it("/health returns 503 when kafka health state is stale", async () => {
    // Construct a kafkaHealthState with timeoutMs=1, sleep 10ms, request /health.
    // Expect status 503.
  });
});
```

The exact mock seam depends on the existing test file's structure. The existing tests likely already inject a fake kafkajs. Mirror that mechanism.

If `setupKafka` does not currently accept a kafka injection point, add an optional 7th option: `kafka?: { consumer, producer }` — defaulting to `new Kafka(...)`. This is a small refactor that keeps tests fast (no real broker needed) and parallels the existing producer/consumer enabled flags.

- [ ] **Step 3: Run tests to verify they fail**

Run: `pnpm -F @canary/order-service test`
Expected: New tests fail (group-id is hardcoded, no filter, no recordPoll, /health unconditional).

- [ ] **Step 4: Modify `kafka.ts` to wire the lib primitives**

Add a return value field for health state and watcher. Replace the file with:

```typescript
import { Kafka, type Producer, type Consumer, type EachMessagePayload } from "kafkajs";
import {
  stampXCanaryOnProducerRecord,
  resolveConsumerGroupId,
  shouldProcess,
  runWithCanaryFromHeaders,
  createKafkaHealthState,
  type KafkaHealthState,
  XCanaryPresenceWatcher,
} from "@canary/lib-node";
import { consumedEventStore } from "./store.js";

export interface KafkaSetupOptions {
  brokers: string[];
  consumersEnabled: boolean;
  producerEnabled: boolean;
  /** Service version: "stable" | "canary". Defaults to process.env.VERSION || "stable". */
  ownVersion?: string;
  /** K8s namespace for the presence watcher. Defaults to process.env.POD_NAMESPACE || "services". */
  namespace?: string;
  /** Disable the presence watcher entirely (e.g., for tests). */
  presenceWatcherEnabled?: boolean;
  /** Override kafka health timeout in ms. Defaults to 30000. */
  kafkaHealthTimeoutMs?: number;
  sendTimeoutMs?: number;
  reconnectIntervalMs?: number;
}

export interface KafkaHandle {
  producer: Producer | null;
  consumer: Consumer | null;
  send: (topic: string, key: string, value: string) => Promise<void>;
  health: KafkaHealthState;
  presenceWatcher: XCanaryPresenceWatcher | null;
}

export async function setupKafka(opts: KafkaSetupOptions): Promise<KafkaHandle> {
  const ownVersion = opts.ownVersion ?? process.env.VERSION ?? "stable";
  const namespace = opts.namespace ?? process.env.POD_NAMESPACE ?? "services";
  const watcherEnabled = opts.presenceWatcherEnabled ?? true;
  const health = createKafkaHealthState(opts.kafkaHealthTimeoutMs ?? 30000);

  const kafka = new Kafka({ clientId: "order-service", brokers: opts.brokers });
  const sendTimeoutMs = opts.sendTimeoutMs ?? 5000;
  const reconnectIntervalMs = opts.reconnectIntervalMs ?? 10000;

  let presenceWatcher: XCanaryPresenceWatcher | null = null;
  if (watcherEnabled && ownVersion === "stable") {
    presenceWatcher = new XCanaryPresenceWatcher(namespace, "order-service");
    await presenceWatcher.start().catch((err) => {
      console.warn(`order-service presence watcher start failed: ${err}; canary will be treated as not-ready`);
    });
  }

  let producer: Producer | null = null;
  let send: KafkaHandle["send"];
  if (opts.producerEnabled) {
    producer = kafka.producer();
    const p = producer;
    let connected = false;
    const connectPromise = (async () => {
      while (true) {
        try {
          await p.connect();
          connected = true;
          console.log("order-service Kafka producer connected");
          return;
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          console.warn(`order-service Kafka producer connect failed: ${msg}; retrying in ${reconnectIntervalMs}ms`);
          await new Promise((r) => setTimeout(r, reconnectIntervalMs));
        }
      }
    })();
    connectPromise.catch(() => {});
    send = async (topic, key, value) => {
      if (!connected) {
        const result = await Promise.race([
          connectPromise.then(() => "ready" as const),
          new Promise<"timeout">((r) => setTimeout(() => r("timeout"), sendTimeoutMs)),
        ]);
        if (result === "timeout") {
          console.warn(`order-service kafka send dropped (producer not connected after ${sendTimeoutMs}ms); topic=${topic} key=${key}`);
          return;
        }
      }
      const record = stampXCanaryOnProducerRecord({ topic, messages: [{ key, value }] });
      await p.send(record);
    };
  } else {
    console.log("KAFKA_PRODUCER_ENABLED=false; producer not started; send() is a no-op");
    send = async () => {};
  }

  let consumer: Consumer | null = null;
  if (opts.consumersEnabled) {
    const groupId = resolveConsumerGroupId("order-service");
    const c = kafka.consumer({ groupId });
    consumer = c;
    const consumerSetupPromise = (async () => {
      while (true) {
        try {
          await c.connect();
          await c.subscribe({ topics: ["payments.events", "inventory.events"] });
          await c.run({
            eachMessage: async ({ topic, message }: EachMessagePayload) => {
              health.recordPoll();
              const isReady = () => (presenceWatcher ? presenceWatcher.isCanaryReady() : false);
              if (!shouldProcess(message.headers, ownVersion, isReady)) {
                return;
              }
              await runWithCanaryFromHeaders(message.headers, async () => {
                const headers: Record<string, string> = {};
                for (const [k, v] of Object.entries(message.headers ?? {})) {
                  if (v) headers[k] = Buffer.isBuffer(v) ? v.toString("utf8") : String(v);
                }
                consumedEventStore.record({
                  topic,
                  key: message.key?.toString("utf8") ?? null,
                  value: message.value?.toString("utf8") ?? "",
                  headers,
                });
              });
            },
          });
          console.log(`order-service Kafka consumer subscribed (groupId=${groupId})`);
          return;
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          console.warn(`order-service Kafka consumer setup failed: ${msg}; retrying in ${reconnectIntervalMs}ms`);
          await new Promise((r) => setTimeout(r, reconnectIntervalMs));
        }
      }
    })();
    consumerSetupPromise.catch(() => {});
  } else {
    console.log("KAFKA_CONSUMERS_ENABLED=false; consumer not started");
  }

  return { producer, consumer, send, health, presenceWatcher };
}
```

Key points:
- `presenceWatcher` is started ONLY on stable pods (canary doesn't watch itself).
- If watcher start fails, treat canary as not ready (defensive — fall back to processing on stable).
- Both stable and canary record a poll on every message → both get the readiness gate (canary's `/health` will fail when its own consumer stalls; stable's also stalls but its readiness probe failure isn't load-bearing for routing).

- [ ] **Step 5: Modify `http.ts` to gate `/health` on Kafka health state**

Change the function signature to accept the health state, and update `/health`:

```typescript
import { type KafkaHealthState } from "@canary/lib-node";

export interface HttpDeps {
  clients: SagaClients;
  kafkaSend?: (topic: string, key: string, value: string) => Promise<void>;
  kafkaHealth?: KafkaHealthState;
}

// In setupHttp(), replace the existing /health line with:
app.get("/health", (_req, res) => {
  const report = deps.kafkaHealth?.report();
  if (report && !report.ok) {
    res.status(503).json({ ok: false, kafka: report });
    return;
  }
  res.json({ ok: true });
});
```

- [ ] **Step 6: Modify `index.ts` to pass health into HTTP and stop watcher on shutdown**

```typescript
import { loadConfig } from "./config.js";
import { setupHttp, buildClient } from "./http.js";
import { setupKafka } from "./kafka.js";
import { setupRestate } from "./restate.js";

const config = loadConfig();

const clients = {
  inventory: buildClient(config.INVENTORY_URL),
  payment: buildClient(config.PAYMENT_URL),
  notification: buildClient(config.NOTIFICATION_URL),
};

const kafka = await setupKafka({
  brokers: config.KAFKA_BOOTSTRAP_SERVERS,
  consumersEnabled: config.KAFKA_CONSUMERS_ENABLED,
  producerEnabled: config.KAFKA_PRODUCER_ENABLED,
});

const app = setupHttp({ clients, kafkaSend: kafka.send, kafkaHealth: kafka.health });

const server = app.listen(config.HTTP_PORT, () => {
  console.log(`order-service HTTP listening on ${config.HTTP_PORT}`);
});

await setupRestate({
  registerHandlers: config.RESTATE_REGISTER_HANDLERS,
  port: config.RESTATE_HANDLER_PORT,
});

const shutdown = async () => {
  console.log("order-service shutting down");
  if (kafka.presenceWatcher) kafka.presenceWatcher.close();
  if (kafka.consumer) await kafka.consumer.disconnect().catch(() => {});
  if (kafka.producer) await kafka.producer.disconnect().catch(() => {});
  server.close();
};
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
```

- [ ] **Step 7: Run tests**

Run: `pnpm -F @canary/order-service test`
Expected: All pass.

- [ ] **Step 8: Build**

Run: `pnpm -F @canary/order-service build`
Expected: tsc clean exit.

- [ ] **Step 9: Commit**

```bash
git add services/order-service/src/kafka.ts \
        services/order-service/src/http.ts \
        services/order-service/src/index.ts \
        services/order-service/src/__tests__/kafka.test.ts
git commit -m "feat(order): wire Phase 2.b canary consumer (group resolver + filter + watcher + health)"
```

---

### Task 5: Wire `notification-service` (Node)

**Files:**
- Modify: `services/notification-service/src/kafka.ts`
- Modify: `services/notification-service/src/http.ts`
- Modify: `services/notification-service/src/index.ts`
- Modify: `services/notification-service/src/__tests__/kafka.test.ts`
- Modify (or extend): `services/notification-service/src/__tests__/http.test.ts` (if it exists; the `/health` 503 test belongs there for cleaner file organization)

**Reference Task 4's commit `1b69a13`** for the working pattern. `git show 1b69a13 -- services/order-service/` shows exactly the substitution-ready code.

**Adopt these refinements** from Task 4's review (don't recreate Task 4's minor smells):

1. **`presenceWatcher.close()` in shutdown is unprotected** — no `try/catch` wrap (`XCanaryPresenceWatcher.close()` already swallows abort errors internally and returns void). Just call it directly:
   ```typescript
   if (kafka.presenceWatcher) kafka.presenceWatcher.close();
   ```
2. **Place the `/health` 503 test in the `http.test.ts` file**, not in `kafka.test.ts`. It exercises `setupHttp`, not `setupKafka`. The other 5 canary tests (group-id resolution + 3 filter cells + recordPoll-on-every-message) belong in `kafka.test.ts`.
3. **The `XCanaryPresenceWatcher` API is positional** — `new XCanaryPresenceWatcher(namespace, "notification-service")`, NOT options-object form.
4. **Test seam**: use `presenceWatcherEnabled: false` + `_testIsCanaryReady` injection (single optional field on `KafkaSetupOptions`, prefix `_test` to signal test-only). Same pattern as order-service.
5. **`process.env.VERSION` must be set BEFORE calling `setupKafka`** — `resolveConsumerGroupId` reads the env at call-time, not module-load. Restore env in `afterEach`.
6. **The `IHeaders` cast for kafkajs → lib** — kafkajs `IHeaders` is `Record<string, Buffer | string | (Buffer | string)[] | undefined>`, while lib's `KafkaConsumeHeaders` is `Record<string, Buffer | undefined>`. The lib only calls `.toString("utf8")` on Buffer values; in this repo all canary headers are written via `stampXCanaryOnProducerRecord` (which writes Buffers). A localized `as KafkaConsumeHeaders` cast with a brief comment explaining the asymmetry is acceptable.

**Service-specific differences from order-service:**
- `clientId: "notification-service"`
- Subscribe topics: `["orders.events", "payments.events"]`
- `resolveConsumerGroupId("notification-service")`
- `serviceName: "notification-service"` for the presence watcher
- Read `services/notification-service/src/index.ts` first to preserve any service-specific bootstrap (downstream clients, restate setup, etc.)

- [ ] **Step 1: Extend `kafka.test.ts` with the 5 kafka-side canary tests**

Mirror order-service's `kafka.test.ts` block. The tests:
- group-id resolution (sets `VERSION=canary`, asserts `kafka.consumer({groupId: "notification-service-canary"})`)
- stable + canaryReady=true skips x-canary message (uses `_testIsCanaryReady: () => true`)
- canary processes x-canary message
- canary skips non-canary message
- recordPoll is called for every message regardless of filter outcome

- [ ] **Step 2: Extend `http.test.ts` with the `/health` 503 test**

Construct a `kafkaHealth` from `createKafkaHealthState(1)` (1ms timeout), record one poll, sleep 10ms, then make a request to `/health`. Expect 503 with `{ ok: false, kafka: ... }`.

If `http.test.ts` does not exist, create it minimally — only this single test.

- [ ] **Step 3: Run tests; confirm new tests fail**

Run: `pnpm -F @canary/notification-service test`
Expected: 6 new tests fail.

- [ ] **Step 4: Modify `kafka.ts`** — apply Task 4 Step 4 structure with the substitutions above.

- [ ] **Step 5: Modify `http.ts`** — `/health` gate exactly as Task 4 Step 5.

- [ ] **Step 6: Modify `index.ts`** — pass `kafkaHealth: kafka.health`; SIGTERM/SIGINT handler that calls `presenceWatcher.close()` (unprotected, see refinement #1), `consumer.disconnect()` (await + catch), `producer.disconnect()` (await + catch), `server.close()`.

- [ ] **Step 7: Run tests**

Run: `pnpm -F @canary/notification-service test`
Expected: All pass.

- [ ] **Step 8: Build**

Run: `pnpm -F @canary/notification-service build`
Expected: tsc clean.

- [ ] **Step 9: Commit**

```bash
git add services/notification-service/src/kafka.ts \
        services/notification-service/src/http.ts \
        services/notification-service/src/index.ts \
        services/notification-service/src/__tests__/kafka.test.ts \
        services/notification-service/src/__tests__/http.test.ts
git commit -m "feat(notification): wire Phase 2.b canary consumer (group resolver + filter + watcher + health)"
```

---

### Task 6: Flip `KAFKA_CONSUMERS_ENABLED=true` on canary overlay

**Files:**
- Modify: `deploy/helm/values/canary-overlay.yaml`

- [ ] **Step 1: Read the current overlay**

Run: `cat deploy/helm/values/canary-overlay.yaml`
Confirm line 17 reads `KAFKA_CONSUMERS_ENABLED: "false"`.

- [ ] **Step 2: Replace `"false"` with `"true"` and update the leading comment block**

Replace the file with:

```yaml
# Canary overlay — applied IN ADDITION to a per-service values file by
# Plan 1.4's canary-ctl. Hard-codes the Phase 2.b canary contract:
#   - canary pods DO consume Kafka (per-subset consumer group <svc>-canary)
#   - canary pods do not register Restate handlers (only stable owns the registry)
#   - canary pods carry the version: canary label so Istio's DestinationRule
#     subset routing can target them
#
# Plan 1.4 runs roughly:
#   helm upgrade --install <svc>-canary deploy/helm/service-chart \
#     -f deploy/helm/values/<svc>.yaml \
#     -f deploy/helm/values/canary-overlay.yaml \
#     --set image.tag=<canary-tag> -n services

version: canary
replicas: 1
env:
  KAFKA_CONSUMERS_ENABLED: "true"
  RESTATE_REGISTER_HANDLERS: "false"
restate:
  registerEndpoint: false
```

- [ ] **Step 3: Render Helm to verify the chart renders cleanly**

Run: `helm template canary-test deploy/helm/service-chart -f deploy/helm/values/order-service.yaml -f deploy/helm/values/canary-overlay.yaml | grep -A1 KAFKA_CONSUMERS_ENABLED`
Expected: shows `KAFKA_CONSUMERS_ENABLED: "true"` in the rendered ConfigMap.

- [ ] **Step 4: Commit**

```bash
git add deploy/helm/values/canary-overlay.yaml
git commit -m "feat(helm): flip canary KAFKA_CONSUMERS_ENABLED=true for Phase 2.b"
```

---

### Task 7: Add e2e helpers for per-pod port-forward + subset-aware consumed-events queries

**Files:**
- Create: `tests/e2e/helpers/pod-port-forward.ts`
- Create: `tests/e2e/helpers/consumed-events.ts`

Why two helpers, not one: the edge Istio gateway in this repo (`deploy/routing/ingress/edge-virtualservice.yaml`) only routes `/api/orders` to order-service. There is NO edge path to other services' `/internal/consumed-events`, and Istio's per-service DestinationRule subsets are only selectable from inside the mesh — not from the test runner. So the only way for a test runner outside the cluster to ask each subset directly is `kubectl port-forward pod/<name>` to a specific pod (stable or canary). The pod-port-forward helper handles the kubectl process; the consumed-events helper builds on top.

Each Java service exposes `/internal/consumed-events` on its container port (audit 8083, payment 8081, inventory 8082). Each Node service exposes it on its container port (order 3001, notification 3002). Note: the Helm chart's container port is set per per-service values file; verify with `grep -A1 ports: deploy/helm/values/<svc>.yaml`.

- [ ] **Step 1: Write `pod-port-forward.ts`**

```typescript
import { spawn, type ChildProcess } from "node:child_process";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import axios from "axios";

const execFileAsync = promisify(execFile);

export interface PodPortForward {
  localPort: number;
  pod: string;
  stop: () => Promise<void>;
}

/**
 * Returns the name of the first pod matching the label selector that is in the
 * Running phase, in the given namespace.
 */
export async function findPodByLabel(namespace: string, selector: string): Promise<string> {
  const { stdout } = await execFileAsync("kubectl", [
    "-n", namespace,
    "get", "pods",
    "-l", selector,
    "--field-selector=status.phase=Running",
    "-o", "jsonpath={.items[0].metadata.name}",
  ]);
  const name = stdout.trim();
  if (!name) throw new Error(`no Running pod matches ${selector} in ${namespace}`);
  return name;
}

/**
 * Starts a `kubectl port-forward pod/<name> <localPort>:<containerPort>` and
 * waits until the local port responds on /health (or any 2xx/3xx/4xx — anything
 * non-network-error means the forward is live). 30s budget.
 */
export async function portForwardPod(
  namespace: string,
  podName: string,
  localPort: number,
  containerPort: number,
): Promise<PodPortForward> {
  const proc: ChildProcess = spawn("kubectl", [
    "-n", namespace,
    "port-forward",
    `pod/${podName}`,
    `${localPort}:${containerPort}`,
  ], { stdio: ["ignore", "pipe", "pipe"] });

  let stderrBuf = "";
  proc.stderr?.on("data", (d) => (stderrBuf += d.toString()));

  // Wait until the port answers
  const deadline = Date.now() + 30_000;
  let lastErr = "";
  while (Date.now() < deadline) {
    try {
      // Any non-network error is fine — we just need TCP to connect.
      await axios.get(`http://localhost:${localPort}/health`, {
        timeout: 1000,
        validateStatus: () => true,
      });
      return {
        localPort,
        pod: podName,
        stop: () => new Promise<void>((res) => {
          if (proc.exitCode != null) { res(); return; }
          proc.once("exit", () => res());
          proc.kill("SIGTERM");
          setTimeout(() => { try { proc.kill("SIGKILL"); } catch {} ; res(); }, 2000);
        }),
      };
    } catch (e) {
      lastErr = e instanceof Error ? e.message : String(e);
      await new Promise((r) => setTimeout(r, 250));
    }
  }
  proc.kill("SIGKILL");
  throw new Error(`port-forward to ${podName}:${containerPort} not ready on :${localPort} within 30s — last err: ${lastErr}; kubectl stderr: ${stderrBuf}`);
}

/** Sends a POSIX signal to PID 1 inside the named pod. Used by K5. */
export async function sendSignalToPod(namespace: string, pod: string, signal: "STOP" | "CONT" | "KILL"): Promise<void> {
  await execFileAsync("kubectl", [
    "-n", namespace,
    "exec", pod,
    "--", "kill", `-${signal}`, "1",
  ]);
}
```

- [ ] **Step 2: Write `consumed-events.ts`**

```typescript
import axios from "axios";
import { findPodByLabel, portForwardPod, type PodPortForward } from "./pod-port-forward.js";

export interface ConsumedEventRow {
  topic: string;
  key: string | null;
  value: string;
  headers: Record<string, string>;
}

/** Container port each service listens on (matches Helm per-service values). */
export const SERVICE_CONTAINER_PORT: Record<string, number> = {
  "order-service": 3001,
  "notification-service": 3002,
  "payment-service": 8081,
  "inventory-service": 8082,
  "audit-service": 8083,
};

/** Local port allocator base — start at 18000 to avoid collisions with traffic.ts (8080) and dashboards. */
let nextLocalPort = 18000;

export async function openSubsetForward(
  service: string,
  subset: "stable" | "canary",
): Promise<PodPortForward> {
  const pod = await findPodByLabel("services", `app=${service},version=${subset}`);
  const containerPort = SERVICE_CONTAINER_PORT[service];
  if (!containerPort) throw new Error(`no container port mapped for service ${service}`);
  const local = nextLocalPort++;
  return portForwardPod("services", pod, local, containerPort);
}

export async function getConsumedEvents(forward: PodPortForward): Promise<ConsumedEventRow[]> {
  const r = await axios.get(`http://localhost:${forward.localPort}/internal/consumed-events`, {
    validateStatus: () => true,
    timeout: 5000,
  });
  if (r.status !== 200) {
    throw new Error(`consumed-events fetch failed (pod=${forward.pod}): status=${r.status} body=${JSON.stringify(r.data)}`);
  }
  if (!Array.isArray(r.data)) {
    throw new Error(`consumed-events response not an array (pod=${forward.pod}): ${JSON.stringify(r.data)}`);
  }
  return r.data as ConsumedEventRow[];
}

export async function waitForConsumed(
  forward: PodPortForward,
  predicate: (rows: ConsumedEventRow[]) => boolean,
  timeoutMs = 15000,
  pollMs = 250,
): Promise<ConsumedEventRow[]> {
  const deadline = Date.now() + timeoutMs;
  let last: ConsumedEventRow[] = [];
  while (Date.now() < deadline) {
    try {
      last = await getConsumedEvents(forward);
      if (predicate(last)) return last;
    } catch (e) {
      // Keep trying — port-forward may have hiccupped briefly.
    }
    await new Promise((r) => setTimeout(r, pollMs));
  }
  throw new Error(`waitForConsumed(pod=${forward.pod}) timed out after ${timeoutMs}ms; last=${JSON.stringify(last).slice(0, 500)}`);
}
```

- [ ] **Step 3: Verify the container ports against the Helm values files**

Run: `for f in deploy/helm/values/*.yaml; do echo "==> $f"; grep -A2 'ports:' "$f"; done`
Expected: Confirms `8081`, `8082`, `8083` for Java services and `3001`, `3002` for Node services. If any differ, update `SERVICE_CONTAINER_PORT`.

- [ ] **Step 4: Commit**

```bash
git add tests/e2e/helpers/pod-port-forward.ts \
        tests/e2e/helpers/consumed-events.ts
git commit -m "test(e2e): add pod-port-forward + consumed-events helpers for Phase 2.b scenarios"
```

---

### Task 8: K1 — canary deployed + flagged event → only canary processes

**Files:**
- Create: `tests/e2e/k1-canary-flagged-event.test.ts`

- [ ] **Step 1: Write the scenario**

```typescript
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { deployCanary, rollback } from "./helpers/canary.js";
import { sendOrder } from "./helpers/traffic.js";
import { openSubsetForward, getConsumedEvents, waitForConsumed } from "./helpers/consumed-events.js";
import type { PodPortForward } from "./helpers/pod-port-forward.js";
import { ensureCleanBaseline, PHASE1_SERVICES } from "./helpers/cluster.js";

describe("K1 — canary deployed + flagged event → only canary processes", () => {
  let auditStable: PodPortForward;
  let auditCanary: PodPortForward;

  beforeAll(async () => {
    await ensureCleanBaseline();
    for (const svc of PHASE1_SERVICES) {
      await deployCanary(svc, "dev");
    }
    auditStable = await openSubsetForward("audit-service", "stable");
    auditCanary = await openSubsetForward("audit-service", "canary");
  }, 600_000);

  afterAll(async () => {
    await auditStable?.stop();
    await auditCanary?.stop();
    for (const svc of PHASE1_SERVICES) {
      await rollback(svc);
    }
  });

  it("canary subset records the consumed event; stable subset does not", async () => {
    const r = await sendOrder({ canary: true, user: "k1-canary-user" });
    expect(r.status).toBeGreaterThanOrEqual(200);
    expect(r.status).toBeLessThan(300);

    const canaryRows = await waitForConsumed(
      auditCanary,
      (rows) => rows.some((row) => row.value.includes("k1-canary-user")),
      15000,
    );
    expect(canaryRows.some((row) => row.headers["x-canary"] === "true")).toBe(true);

    const stableRows = await getConsumedEvents(auditStable);
    expect(stableRows.some((row) => row.value.includes("k1-canary-user"))).toBe(false);
  });
});
```

- [ ] **Step 2: Run the scenario (gated by `E2E_SCENARIOS=1`)**

Run: `E2E_SCENARIOS=1 pnpm -F @canary/e2e test k1-canary-flagged-event`
Expected: passes (assumes 2.b service code is deployed; if not yet image-rebuilt, see Task 13).

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/k1-canary-flagged-event.test.ts
git commit -m "test(e2e): K1 — canary deployed + flagged event routes to canary subset"
```

---

### Task 9: K2 — canary deployed + unflagged event → only stable processes

**Files:**
- Create: `tests/e2e/k2-canary-unflagged-event.test.ts`

- [ ] **Step 1: Write the scenario**

```typescript
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { deployCanary, rollback } from "./helpers/canary.js";
import { sendOrder } from "./helpers/traffic.js";
import { openSubsetForward, getConsumedEvents, waitForConsumed } from "./helpers/consumed-events.js";
import type { PodPortForward } from "./helpers/pod-port-forward.js";
import { ensureCleanBaseline, PHASE1_SERVICES } from "./helpers/cluster.js";

describe("K2 — canary deployed + unflagged event → only stable processes", () => {
  let auditStable: PodPortForward;
  let auditCanary: PodPortForward;

  beforeAll(async () => {
    await ensureCleanBaseline();
    for (const svc of PHASE1_SERVICES) {
      await deployCanary(svc, "dev");
    }
    auditStable = await openSubsetForward("audit-service", "stable");
    auditCanary = await openSubsetForward("audit-service", "canary");
  }, 600_000);

  afterAll(async () => {
    await auditStable?.stop();
    await auditCanary?.stop();
    for (const svc of PHASE1_SERVICES) {
      await rollback(svc);
    }
  });

  it("stable subset records the consumed event; canary subset does not", async () => {
    const r = await sendOrder({ canary: false, user: "k2-stable-user" });
    expect(r.status).toBeGreaterThanOrEqual(200);
    expect(r.status).toBeLessThan(300);

    const stableRows = await waitForConsumed(
      auditStable,
      (rows) => rows.some((row) => row.value.includes("k2-stable-user")),
      15000,
    );
    expect(stableRows.length).toBeGreaterThan(0);

    const canaryRows = await getConsumedEvents(auditCanary);
    expect(canaryRows.some((row) => row.value.includes("k2-stable-user"))).toBe(false);
  });
});
```

- [ ] **Step 2: Run**

Run: `E2E_SCENARIOS=1 pnpm -F @canary/e2e test k2-canary-unflagged-event`
Expected: passes.

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/k2-canary-unflagged-event.test.ts
git commit -m "test(e2e): K2 — unflagged event with canary deployed routes to stable subset"
```

---

### Task 10: K3 — canary NOT deployed + flagged event → stable falls back

**Files:**
- Create: `tests/e2e/k3-no-canary-fallback.test.ts`

- [ ] **Step 1: Write the scenario**

```typescript
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { sendOrder } from "./helpers/traffic.js";
import { openSubsetForward, waitForConsumed } from "./helpers/consumed-events.js";
import type { PodPortForward } from "./helpers/pod-port-forward.js";
import { ensureCleanBaseline } from "./helpers/cluster.js";

describe("K3 — canary NOT deployed + flagged event → stable falls back", () => {
  let auditStable: PodPortForward;

  beforeAll(async () => {
    await ensureCleanBaseline();
    // Intentionally do NOT deploy any canary subsets.
    auditStable = await openSubsetForward("audit-service", "stable");
  }, 300_000);

  afterAll(async () => {
    await auditStable?.stop();
  });

  it("flagged event lands on stable subset because canary is absent", async () => {
    const r = await sendOrder({ canary: true, user: "k3-fallback-user" });
    expect(r.status).toBeGreaterThanOrEqual(200);
    expect(r.status).toBeLessThan(300);

    const rows = await waitForConsumed(
      auditStable,
      (rows) => rows.some((row) => row.value.includes("k3-fallback-user")),
      15000,
    );
    expect(rows.length).toBeGreaterThan(0);
  });
});
```

- [ ] **Step 2: Run**

Run: `E2E_SCENARIOS=1 pnpm -F @canary/e2e test k3-no-canary-fallback`
Expected: passes.

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/k3-no-canary-fallback.test.ts
git commit -m "test(e2e): K3 — flagged event without canary falls back to stable"
```

---

### Task 11: K4 — header propagates from Kafka consume → downstream

**Files:**
- Create: `tests/e2e/k4-kafka-header-propagation.test.ts`

The hypothesis: when canary's audit-service consumer processes an `x-canary=true` event, any HTTP/Kafka/Restate calls it makes downstream inherit the canary header. Audit-service has no downstream HTTP, but **payment-service** does (it consumes `orders.events` and may produce downstream); **order-service** consumes `payments.events` + `inventory.events` and re-stamps any further events. Verify by inspecting the downstream consumed event headers.

Practical check: send a flagged event, observe that the downstream Kafka events propagated by the canary chain ALSO carry `x-canary=true` in their headers (recorded at downstream consumers' `consumedEventStore`).

- [ ] **Step 1: Write the scenario**

```typescript
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { deployCanary, rollback } from "./helpers/canary.js";
import { sendOrder } from "./helpers/traffic.js";
import { openSubsetForward, waitForConsumed } from "./helpers/consumed-events.js";
import type { PodPortForward } from "./helpers/pod-port-forward.js";
import { ensureCleanBaseline, PHASE1_SERVICES } from "./helpers/cluster.js";

describe("K4 — Kafka consume context propagates x-canary downstream", () => {
  let auditCanary: PodPortForward;
  let orderCanary: PodPortForward;

  beforeAll(async () => {
    await ensureCleanBaseline();
    for (const svc of PHASE1_SERVICES) {
      await deployCanary(svc, "dev");
    }
    auditCanary = await openSubsetForward("audit-service", "canary");
    orderCanary = await openSubsetForward("order-service", "canary");
  }, 600_000);

  afterAll(async () => {
    await auditCanary?.stop();
    await orderCanary?.stop();
    for (const svc of PHASE1_SERVICES) {
      await rollback(svc);
    }
  });

  it("canary's audit-service downstream events carry x-canary=true", async () => {
    const r = await sendOrder({ canary: true, user: "k4-propagation-user" });
    expect(r.status).toBeGreaterThanOrEqual(200);
    expect(r.status).toBeLessThan(300);

    const auditRows = await waitForConsumed(
      auditCanary,
      (rows) => rows.some((row) => row.value.includes("k4-propagation-user")),
      15000,
    );
    const matchedAudit = auditRows.find((r) => r.value.includes("k4-propagation-user"));
    expect(matchedAudit?.headers["x-canary"]).toBe("true");

    // The downstream events the chain emits (e.g., payments.events, inventory.events)
    // must carry x-canary=true at their canary consumers. order-service canary consumes
    // payments.events + inventory.events; verify the header is present there.
    const orderRows = await waitForConsumed(
      orderCanary,
      (rows) => rows.some((row) => row.value.includes("k4-propagation-user")),
      15000,
    );
    const matchedOrder = orderRows.find((r) => r.value.includes("k4-propagation-user"));
    expect(matchedOrder?.headers["x-canary"]).toBe("true");
  });
});
```

- [ ] **Step 2: Run**

Run: `E2E_SCENARIOS=1 pnpm -F @canary/e2e test k4-kafka-header-propagation`
Expected: passes. If a service drops the canary header on a downstream emit, the test will surface it.

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/k4-kafka-header-propagation.test.ts
git commit -m "test(e2e): K4 — x-canary propagates through Kafka consume → downstream"
```

---

### Task 12: K5 — canary readiness gating (Kafka health failure → stable takes over)

**Files:**
- Create: `tests/e2e/k5-canary-kafka-unhealthy.test.ts`

K5 verifies: when canary's Kafka consumer stops polling (simulated by SIGSTOP-ing the canary pod's process), the readiness probe fails, the kubelet marks the pod NotReady, the stable's presence watcher fires, and stable picks up canary-flagged events.

Mechanism: `kubectl exec` into the canary pod and send `SIGSTOP` to PID 1. That halts both the HTTP server and the Kafka client → the readiness probe times out → kubelet marks the pod NotReady. Stable's pod watch on `app=audit-service,version=canary` sees the Ready transition flip to False and updates the in-memory flag. Subsequent canary-flagged events land on stable. After the test, send `SIGCONT` and roll back. `findPodByLabel` and `sendSignalToPod` are already provided by `pod-port-forward.ts` from Task 7.

Important: query the audit-service stable forward BEFORE sending SIGSTOP to canary — once SIGSTOP'd, opening a forward to the canary pod hangs (the kubelet can't probe and the pod's readiness flips). This scenario only forwards to stable.

- [ ] **Step 1: Write the scenario**

```typescript
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { deployCanary, rollback } from "./helpers/canary.js";
import { sendOrder } from "./helpers/traffic.js";
import { openSubsetForward, waitForConsumed } from "./helpers/consumed-events.js";
import { findPodByLabel, sendSignalToPod, type PodPortForward } from "./helpers/pod-port-forward.js";
import { ensureCleanBaseline } from "./helpers/cluster.js";

describe("K5 — canary Kafka unhealthy → stable takes over flagged events", () => {
  let canaryPod = "";
  let auditStable: PodPortForward;
  let auditCanary: PodPortForward | null = null;

  beforeAll(async () => {
    await ensureCleanBaseline();
    await deployCanary("audit-service", "dev");
    canaryPod = await findPodByLabel("services", "app=audit-service,version=canary");
    auditStable = await openSubsetForward("audit-service", "stable");
    auditCanary = await openSubsetForward("audit-service", "canary");
  }, 300_000);

  afterAll(async () => {
    // Best-effort revive in case the test left the pod in STOP state.
    if (canaryPod) { try { await sendSignalToPod("services", canaryPod, "CONT"); } catch {} }
    await auditStable?.stop();
    await auditCanary?.stop().catch(() => {});
    await rollback("audit-service");
  });

  it("after SIGSTOP on canary, stable processes a flagged event", async () => {
    // Step A: baseline — flagged event goes to canary while canary is healthy
    const baseline = await sendOrder({ canary: true, user: "k5-baseline" });
    expect(baseline.status).toBeGreaterThanOrEqual(200);
    expect(baseline.status).toBeLessThan(300);
    await waitForConsumed(
      auditCanary!,
      (rows) => rows.some((r) => r.value.includes("k5-baseline")),
      15000,
    );

    // Step B: stop the canary process — readiness will fail within KAFKA_HEALTH_TIMEOUT_MS,
    // then kubelet probe failureThreshold × periodSeconds, then watch propagation.
    await sendSignalToPod("services", canaryPod, "STOP");
    // The canary forward will hang once SIGSTOP'd; close it so vitest doesn't deadlock.
    await auditCanary!.stop().catch(() => {});
    auditCanary = null;

    // Default kafka health timeout 30s + probe failureThreshold 3 × periodSeconds 5 = 15s
    // + watch propagation ~1s. Allow up to 60s.
    await new Promise((r) => setTimeout(r, 60_000));

    // Step C: send a flagged event — should now land on stable
    const flagged = await sendOrder({ canary: true, user: "k5-fallback" });
    expect(flagged.status).toBeGreaterThanOrEqual(200);
    expect(flagged.status).toBeLessThan(300);

    const stableRows = await waitForConsumed(
      auditStable,
      (rows) => rows.some((r) => r.value.includes("k5-fallback")),
      30000,
    );
    expect(stableRows.some((r) => r.value.includes("k5-fallback"))).toBe(true);

    // Step D: revive canary so the cluster returns to a clean state
    await sendSignalToPod("services", canaryPod, "CONT");
  }, 180_000);
});
```

- [ ] **Step 2: Run**

Run: `E2E_SCENARIOS=1 pnpm -F @canary/e2e test k5-canary-kafka-unhealthy`
Expected: passes (the 60s wait covers the readiness flip + watch propagation).

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/k5-canary-kafka-unhealthy.test.ts
git commit -m "test(e2e): K5 — canary Kafka health failure → stable takes over flagged events"
```

---

### Task 13: Build images, deploy, and run the full Phase 2 e2e suite

**Files:** none — orchestration step.

- [ ] **Step 1: Run all unit tests**

Run: `make verify`
Expected: all Java + Node unit tests pass.

- [ ] **Step 2: Build service images**

Run: `make build-services && make build-images && make load-images`
Expected: 5 service images rebuilt and loaded into the kind cluster.

- [ ] **Step 3: Redeploy stable services**

Run: `make deploy-services`
Expected: stable Deployments rollout-restarted; canary subsets (if any from prior tests) untouched.

- [ ] **Step 4: Run the full Phase 2 e2e suite**

Run: `E2E_SCENARIOS=1 pnpm -F @canary/e2e test -- --grep '^K[1-5]'`
Expected: K1–K5 all pass.

- [ ] **Step 5: Run the full e2e suite (Phase 1 + Phase 2) to ensure no regressions**

Run: `E2E_SCENARIOS=1 pnpm -F @canary/e2e test`
Expected: all S1–S13 + K1–K5 pass.

- [ ] **Step 6: Inspect logs spot-check (manual)**

Run: `kubectl -n services logs deploy/audit-service-stable | grep canary`
Expected: log lines from the stable subset showing it skipped canary-flagged messages while canary was Ready (during K1).

No commit for this task — it's a verification step. If any test fails, fix the underlying issue and amend the corresponding earlier task's commit (or land a follow-up commit referencing the cause).

---

### Task 14: Update README with Plan 2.b section

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Read the existing Plan 2.a section to mirror format**

Run: `sed -n '283,320p' README.md`

- [ ] **Step 2: Append a Plan 2.b section after Plan 2.a**

Add directly after the Plan 2.a "Next:" line:

```markdown
## Plan 2.b — Service integration + Phase 2 e2e (complete)

Plan 2.b consumes the Plan 2.a foundation. All 5 services now resolve their Kafka consumer group ID per subset (stable → `<svc>-stable`, canary → `<svc>-canary`), gate each message on `XCanaryConsumeFilter` / `shouldProcess`, propagate `x-canary` into the consume context (so downstream HTTP/Kafka/Restate calls inherit it), and record poll timestamps that flow into the readiness probe (Java: actuator `kafkaConsumer` indicator in the readiness group; Node: `/health` returns 503 when the in-memory health state reports stale).

The canary overlay (`deploy/helm/values/canary-overlay.yaml`) flips `KAFKA_CONSUMERS_ENABLED` from `"false"` to `"true"`. Per-subset consumer groups are created by Kafka on first poll; no new KafkaTopic CRDs are needed.

Phase 2 acceptance scenarios K1–K5 (under `tests/e2e/`) prove the four canary rules end-to-end:

- **K1** — canary deployed + flagged event → only canary's `consumedEventStore` records it
- **K2** — canary deployed + unflagged event → only stable's store records it
- **K3** — canary NOT deployed + flagged event → stable's store records it (graceful fallback)
- **K4** — flagged event consumed by canary's audit-service → downstream Kafka events at canary-side consumers carry `x-canary: true`
- **K5** — canary process SIGSTOP'd → readiness probe fails → stable's pod watch flips → stable processes the next flagged event

Subset-aware verification uses `kubectl port-forward pod/<name>` to each subset's pod (via `tests/e2e/helpers/pod-port-forward.ts`) and queries `/internal/consumed-events` directly — the edge gateway only routes `/api/orders`, and Istio subset-by-header is in-mesh-only, so the test runner has to address pods individually.

Phase 2 (Kafka canary) is now feature-complete. Schema evolution (Phase 2.c) is deferred.
```

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs(readme): add Plan 2.b section (service integration + K1–K5 e2e)"
```

---

## Self-review checklist (run before declaring done)

Run these after Task 14:

1. **Spec coverage:**
   - [ ] All 5 services wired (Tasks 1–5).
   - [ ] Canary overlay flipped (Task 6).
   - [ ] All 5 K-scenarios authored (Tasks 8–12).
   - [ ] README updated (Task 14).
2. **No placeholders:**
   - [ ] Every test in K1–K5 has actual assertions, not "TODO write assertion".
   - [ ] Every modified service file shows full final contents (not "add a few lines").
3. **Type/name consistency:**
   - [ ] `XCanaryConsumeFilter` (not `XCanaryConsumerFilter` — note the singular Consume) used everywhere in Java.
   - [ ] `resolveConsumerGroupId` (not `resolveCanaryConsumerGroupId`) used everywhere in Node.
   - [ ] `runWithCanaryFromHeaders` (not `runWithCanaryHeader`) in Node.
   - [ ] `createKafkaHealthState` (not `createKafkaConsumerHealth`) in Node.
4. **Verification commands:**
   - [ ] `make verify` is run in Task 13.
   - [ ] Full e2e suite is run in Task 13.

If any item is unchecked, return to the relevant task and fix.

## Out-of-scope (explicit)

- Schema evolution / schema registry / canary-aware schema versions → Plan 2.c (future).
- Kafka admin-API offset transfer when canary terminates with un-acked messages → operator-triggered rollback handles for now.
- Auto-cleanup of `<svc>-canary` consumer group after rollback → Kafka group GC handles eventually; not in 2.b.
- Reasonably testing the in-cluster k8s API watch failure modes (network partition, API server crash) → covered by lib-level unit tests in 2.a; not re-tested at e2e level here.
