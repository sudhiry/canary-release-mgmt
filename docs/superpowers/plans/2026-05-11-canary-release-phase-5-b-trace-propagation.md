# Canary Release Phase 5.b — Trace Propagation + Restate Handler Metric Wiring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a canary request observable as one connected Jaeger trace across HTTP → Kafka → Restate → Kafka, AND emit per-handler `canary_request_total{substrate="restate", target=<handler>}` metrics for every Restate handler invocation in all 5 services. Java + Node simultaneously.

**Architecture:** Three classes of change. (1) Restate runtime tracing turned on via env var on the StatefulSet. (2) Spring Kafka observation flag enabled on the listener factory and every KafkaTemplate, so producers stamp `traceparent` and consumers create child spans (Java side). KafkaJS auto-instrumentation does the same for Node — verified at runtime. (3) Restate handler bodies wrapped with the `CanaryRestateMeter.measure(...)` / `measureRestate(...)` helpers shipped in 5.a / 5.a-node, so per-handler latency + outcome metrics flow.

**Tech Stack:** Same as 5.a + 5.a-node. No new deps.

---

## File Structure

### Modified

| Path | Change |
|---|---|
| `deploy/kind/restate/statefulset.yaml` | Add `RESTATE_TRACING_ENDPOINT` and `RESTATE_TRACING_FILTER` env vars. |
| `platform/lib-java/src/main/java/com/canary/platform/lib/autoconfigure/XCanaryAutoConfiguration.java` | Add `factory.getContainerProperties().setObservationEnabled(true)` on the `kafkaListenerContainerFactory` bean. |
| `services/payment-service/src/main/java/com/canary/payment/kafka/KafkaProducerConfig.java` | Add `template.setObservationEnabled(true)` on the `KafkaTemplate` bean. |
| `services/audit-service/src/main/java/com/canary/audit/kafka/KafkaProducerConfig.java` | Same. |
| `services/inventory-service/src/main/java/com/canary/inventory/kafka/KafkaProducerConfig.java` | Same. |
| `services/payment-service/src/main/java/com/canary/payment/handler/PaymentVOImplStable.java` | Inject `CanaryRestateMeter`; wrap `charge` and `refund` bodies. |
| `services/payment-service/src/main/java/com/canary/payment/handler/PaymentVOImplCanary.java` | Same shape; handler-name target uses `PaymentVOCanary.*`. |
| `services/payment-service/src/main/java/com/canary/payment/config/RestateEndpointConfig.java` | Constructor wiring to pass the meter into the Impl beans. |
| `services/inventory-service/src/main/java/com/canary/inventory/handler/ReservationWorkflowImplStable.java` | Wrap `run`, `confirm`, `release` with the meter. |
| `services/inventory-service/src/main/java/com/canary/inventory/handler/ReservationWorkflowImplCanary.java` | Same; canary handler-name target. |
| `services/inventory-service/src/main/java/com/canary/inventory/config/RestateEndpointConfig.java` | Constructor wiring. |
| `services/audit-service/src/main/java/com/canary/audit/handler/AuditQueryServiceImpl.java` | Wrap `append`, `byAggregate`. |
| `services/audit-service/src/main/java/com/canary/audit/config/RestateEndpointConfig.java` | Constructor wiring. |
| `services/order-service/src/restate.ts` | Wrap `checkoutSagaRunHandler` body in `await measureRestate(metrics, ...)`. Add `metrics: CanaryMetrics` to `RestateSetupOptions`. |
| `services/order-service/src/index.ts` | Pass `metrics` into `setupRestate({...})`. |
| `services/notification-service/src/restate.ts` | Wrap `notifyHandler`. Add `metrics` option. |
| `services/notification-service/src/index.ts` | Pass `metrics` into `setupRestate`. |

### Conditional (only if verification at Task 9 / 10 fails)

| Path | Change (only on fallback) |
|---|---|
| `platform/lib-java/src/main/java/com/canary/platform/lib/XCanaryRestateClientCustomizer.java` | Add `traceparent` injection alongside the existing `x-canary` header stamping. |
| `platform/lib-node/src/x-canary-restate.ts` | `applyXCanaryToRestateOptions` extended to inject `traceparent`. |
| `platform/lib-node/src/observability/canary-kafka-metrics.ts` | Add producer wrap helper that injects `traceparent` into Kafka headers. |

---

## Task 1 — Set `RESTATE_TRACING_ENDPOINT` on the Restate StatefulSet

**Files:**
- Modify: `deploy/kind/restate/statefulset.yaml`

- [ ] **Step 1.1: Apply the env change**

Read the file first. The current `env:` block is:

```yaml
env:
  # RESTATE_TRACING_ENDPOINT is intentionally omitted: 1.6.x panics on
  # an empty-string URI (regression vs 1.1.x). Omitting it disables
  # tracing export without error.
  - name: RUST_LOG
    value: info
```

Replace with (preserve the existing comment as historical context, then turn it on):

```yaml
env:
  # Phase 5.b enables tracing export. The 1.6.x empty-string panic
  # documented previously is avoided by setting a real endpoint.
  - name: RUST_LOG
    value: info
  - name: RESTATE_TRACING_ENDPOINT
    value: "http://jaeger-collector.istio-system:4317"
  - name: RESTATE_TRACING_FILTER
    value: "info,restate=info"
```

- [ ] **Step 1.2: Validate yaml renders cleanly**

Run: `kubectl apply --dry-run=client -f deploy/kind/restate/statefulset.yaml -o yaml | head -50` (if `kubectl` is on PATH). If `kubectl` is not available locally, parse-validate via `python3 -c 'import yaml,sys; yaml.safe_load(open(sys.argv[1]))' deploy/kind/restate/statefulset.yaml` — should print nothing on success.

- [ ] **Step 1.3: Commit**

```bash
git add deploy/kind/restate/statefulset.yaml
git commit -m "feat(observability): turn on Restate runtime tracing → Jaeger OTLP"
```

---

## Task 2 — Enable Spring Kafka observation (Java)

**Files:**
- Modify: `platform/lib-java/src/main/java/com/canary/platform/lib/autoconfigure/XCanaryAutoConfiguration.java`
- Modify: `platform/lib-java/src/test/java/com/canary/platform/lib/XCanaryAutoConfigurationTest.java`
- Modify: `services/payment-service/src/main/java/com/canary/payment/kafka/KafkaProducerConfig.java`
- Modify: `services/audit-service/src/main/java/com/canary/audit/kafka/KafkaProducerConfig.java`
- Modify: `services/inventory-service/src/main/java/com/canary/inventory/kafka/KafkaProducerConfig.java`

- [ ] **Step 2.1: Add observation flag to listener factory**

Read `XCanaryAutoConfiguration.java` first. Find the `kafkaListenerContainerFactory` bean method (currently sets `consumerRebalanceListener` and `recordInterceptor`). Add ONE line before `return factory;`:

```java
factory.getContainerProperties().setObservationEnabled(true);
```

- [ ] **Step 2.2: Add a test asserting observation is enabled**

Add to `XCanaryAutoConfigurationTest.java`:

```java
@Test
void kafkaListenerContainerFactoryHasObservationEnabled() {
    runner
        .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
        .withPropertyValues("canary.service-name=payment", "canary.presence-watcher.enabled=false")
        .run(ctx -> {
            ConcurrentKafkaListenerContainerFactory<?, ?> factory = ctx.getBean(
                    "kafkaListenerContainerFactory",
                    ConcurrentKafkaListenerContainerFactory.class);
            assertThat(factory.getContainerProperties().isObservationEnabled()).isTrue();
        });
}
```

- [ ] **Step 2.3: Run the test, verify it FAILS first if the order is reversed (or just verify it now passes)**

Run: `./gradlew :platform:lib-java:test --tests "com.canary.platform.lib.XCanaryAutoConfigurationTest"`
Expected: BUILD SUCCESSFUL — the test now passes because Step 2.1 already enabled observation. (TDD-strict would write the test first; for a single-line config flag we accept the consolidated step.)

- [ ] **Step 2.4: Add observation to each service's KafkaTemplate**

Edit each of the three `KafkaProducerConfig.java` files. Find the `kafkaTemplate(...)` bean method (one-liner returning `new KafkaTemplate<>(pf)`). Replace with:

```java
@Bean
public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> pf) {
    KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);
    template.setObservationEnabled(true);
    return template;
}
```

- [ ] **Step 2.5: Build all 3 Java services**

Run: `./gradlew :services:payment-service:bootJar :services:audit-service:bootJar :services:inventory-service:bootJar`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 2.6: Run full lib-java test suite**

Run: `./gradlew :platform:lib-java:test`
Expected: BUILD SUCCESSFUL — no regressions.

- [ ] **Step 2.7: Commit**

```bash
git add platform/lib-java/src/main/java/com/canary/platform/lib/autoconfigure/XCanaryAutoConfiguration.java \
        platform/lib-java/src/test/java/com/canary/platform/lib/XCanaryAutoConfigurationTest.java \
        services/payment-service/src/main/java/com/canary/payment/kafka/KafkaProducerConfig.java \
        services/audit-service/src/main/java/com/canary/audit/kafka/KafkaProducerConfig.java \
        services/inventory-service/src/main/java/com/canary/inventory/kafka/KafkaProducerConfig.java
git commit -m "feat(observability): enable Spring Kafka observation on listener factory + all KafkaTemplates"
```

---

## Task 3 — Wrap payment-service Restate handlers with the meter

**Files:**
- Modify: `services/payment-service/src/main/java/com/canary/payment/handler/PaymentVOImplStable.java`
- Modify: `services/payment-service/src/main/java/com/canary/payment/handler/PaymentVOImplCanary.java`
- Modify: `services/payment-service/src/main/java/com/canary/payment/config/RestateEndpointConfig.java`
- Add tests as appropriate (see Step 3.5)

Background: `PaymentVOImplStable` is a Restate-binding subclass that delegates each `@Override`'d handler method to `PaymentVOCore`. We wrap the delegate call with `restateMeter.measure("PaymentVOStable.charge", () -> core.charge(req))`.

`CanaryRestateMeter.measure` has signature `<T> T measure(String handlerName, ThrowingSupplier<T> body) throws Exception`. Since `void` handlers can't return a value, wrap them with a `(Void) measure(name, () -> { core.release(); return null; })` pattern.

- [ ] **Step 3.1: Update `PaymentVOImplStable`**

Replace the file with:

```java
package com.canary.payment.handler;

import com.canary.payment.store.ChargeStore;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import com.canary.platform.lib.observability.CanaryRestateMeter;
import com.canary.restate.payment.Charge;
import com.canary.restate.payment.ChargeRequest;
import com.canary.restate.payment.PaymentVOStable;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Restate-binding subclass for the stable variant. Delegates all handlers to a
 * shared {@link PaymentVOCore} instance; each delegate call is wrapped with
 * {@link CanaryRestateMeter#measure} so per-handler latency + outcome metrics flow.
 */
public class PaymentVOImplStable extends PaymentVOStable {
    private final PaymentVOCore core;
    private final CanaryRestateMeter meter;

    public PaymentVOImplStable(ChargeStore store, XCanaryRestateClientCustomizer canary,
                                KafkaTemplate<String, String> kafkaTemplate,
                                ObjectMapper objectMapper,
                                CanaryRestateMeter meter) {
        this.core = new PaymentVOCore(store, canary, kafkaTemplate, objectMapper, false);
        this.meter = meter;
    }

    @Override
    public Charge charge(ChargeRequest req) {
        try {
            return meter.measure("PaymentVOStable.charge", () -> core.charge(req));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Charge refund(ChargeRequest req) {
        try {
            return meter.measure("PaymentVOStable.refund", () -> core.refund(req));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
```

- [ ] **Step 3.2: Update `PaymentVOImplCanary` identically with `PaymentVOCanary.*` handler names**

Same shape; replace `"PaymentVOStable.charge"` → `"PaymentVOCanary.charge"`, `"PaymentVOStable.refund"` → `"PaymentVOCanary.refund"`. The constructor passes `true` to `PaymentVOCore`. Keep the existing class-level Javadoc.

- [ ] **Step 3.3: Update `RestateEndpointConfig` to inject the meter**

Read the file first. Find the bean methods that instantiate `PaymentVOImplStable` and `PaymentVOImplCanary`. Add `CanaryRestateMeter meter` as a constructor parameter to each `@Bean` method, and pass it into the `new PaymentVOImpl*(...)` call. Spring will autowire the `CanaryRestateMeter` bean (registered by `CanaryMetricsAutoConfiguration` from 5.a).

- [ ] **Step 3.4: Build the service**

Run: `./gradlew :services:payment-service:bootJar`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 3.5: Run existing payment-service tests**

Run: `./gradlew :services:payment-service:test`
Expected: BUILD SUCCESSFUL. If existing tests construct `PaymentVOImplStable/Canary` directly with the old constructor signature, they will fail to compile — update each call site to pass a `CanaryRestateMeter`. For tests that don't care about the metric, pass `new CanaryRestateMeter(new CanaryMetrics(new SimpleMeterRegistry(), "payment"))`. If the test count was X before, it should still be X (or +0) after.

- [ ] **Step 3.6: Commit**

```bash
git add services/payment-service/src/main/java/com/canary/payment/handler/PaymentVOImplStable.java \
        services/payment-service/src/main/java/com/canary/payment/handler/PaymentVOImplCanary.java \
        services/payment-service/src/main/java/com/canary/payment/config/RestateEndpointConfig.java \
        services/payment-service/src/test/
git commit -m "feat(observability): wrap PaymentVO handlers with CanaryRestateMeter"
```

(Adjust the test path glob to match files actually changed.)

---

## Task 4 — Wrap inventory-service Restate handlers

**Files:**
- Modify: `services/inventory-service/src/main/java/com/canary/inventory/handler/ReservationWorkflowImplStable.java`
- Modify: `services/inventory-service/src/main/java/com/canary/inventory/handler/ReservationWorkflowImplCanary.java`
- Modify: `services/inventory-service/src/main/java/com/canary/inventory/config/RestateEndpointConfig.java`

Same pattern as Task 3, but for three handler methods per variant: `run(req)`, `confirm()`, `release()`. The `release()` method returns `void` — wrap it with the `Void` pattern:

```java
@Override
public void release() {
    try {
        meter.measure("ReservationWorkflowStable.release", () -> { core.release(); return null; });
    } catch (RuntimeException e) {
        throw e;
    } catch (Exception e) {
        throw new RuntimeException(e);
    }
}
```

`run` and `confirm` use the regular `<Reservation>` returning pattern.

- [ ] **Step 4.1: Update `ReservationWorkflowImplStable`**

Read the existing file first. Apply the same constructor-extension pattern as Task 3.1, then wrap each of the three `@Override` methods. Handler-name targets: `ReservationWorkflowStable.run`, `ReservationWorkflowStable.confirm`, `ReservationWorkflowStable.release`.

- [ ] **Step 4.2: Update `ReservationWorkflowImplCanary`**

Identical shape; canary handler-name targets (`ReservationWorkflowCanary.*`).

- [ ] **Step 4.3: Update `RestateEndpointConfig` to inject the meter**

Same approach as Task 3.3.

- [ ] **Step 4.4: Build + test**

Run: `./gradlew :services:inventory-service:bootJar :services:inventory-service:test`
Expected: BUILD SUCCESSFUL. Update test fixtures that construct the Impl classes directly (same pattern as Task 3.5).

- [ ] **Step 4.5: Commit**

```bash
git add services/inventory-service/src/main/java/com/canary/inventory/handler/ReservationWorkflowImplStable.java \
        services/inventory-service/src/main/java/com/canary/inventory/handler/ReservationWorkflowImplCanary.java \
        services/inventory-service/src/main/java/com/canary/inventory/config/RestateEndpointConfig.java \
        services/inventory-service/src/test/
git commit -m "feat(observability): wrap ReservationWorkflow handlers with CanaryRestateMeter"
```

---

## Task 5 — Wrap audit-service Restate handlers

**Files:**
- Modify: `services/audit-service/src/main/java/com/canary/audit/handler/AuditQueryServiceImpl.java`
- Modify: `services/audit-service/src/main/java/com/canary/audit/config/RestateEndpointConfig.java`

audit-service has only one Impl (no stable/canary split — it's a single Restate service). Two handler methods: `append(event)` (void), `byAggregate(aggregateId)` (returns List).

- [ ] **Step 5.1: Update `AuditQueryServiceImpl`**

Replace the file:

```java
package com.canary.audit.handler;

import com.canary.audit.store.AuditEventStore;
import com.canary.platform.lib.observability.CanaryRestateMeter;
import com.canary.restate.audit.AuditEvent;
import com.canary.restate.audit.AuditQueryService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;

public class AuditQueryServiceImpl extends AuditQueryService {

    private final AuditEventStore store;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final CanaryRestateMeter meter;

    public AuditQueryServiceImpl(AuditEventStore store,
                                 KafkaTemplate<String, String> kafkaTemplate,
                                 ObjectMapper objectMapper,
                                 CanaryRestateMeter meter) {
        this.store = store;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.meter = meter;
    }

    @Override
    public void append(AuditEvent event) {
        try {
            meter.measure("AuditQueryService.append", () -> {
                store.append(event);
                try {
                    String json = objectMapper.writeValueAsString(event);
                    kafkaTemplate.send("audit.events", event.id(), json);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException("Failed to serialize AuditEvent", e);
                }
                return null;
            });
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<AuditEvent> byAggregate(String aggregateId) {
        try {
            return meter.measure("AuditQueryService.byAggregate", () -> store.findByAggregate(aggregateId));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
```

- [ ] **Step 5.2: Update `RestateEndpointConfig`**

Same approach as Task 3.3.

- [ ] **Step 5.3: Build + test**

Run: `./gradlew :services:audit-service:bootJar :services:audit-service:test`
Expected: BUILD SUCCESSFUL. Update tests if they construct `AuditQueryServiceImpl` directly.

- [ ] **Step 5.4: Commit**

```bash
git add services/audit-service/src/main/java/com/canary/audit/handler/AuditQueryServiceImpl.java \
        services/audit-service/src/main/java/com/canary/audit/config/RestateEndpointConfig.java \
        services/audit-service/src/test/
git commit -m "feat(observability): wrap AuditQueryService handlers with CanaryRestateMeter"
```

---

## Task 6 — Wrap order-service Restate handler

**Files:**
- Modify: `services/order-service/src/restate.ts`
- Modify: `services/order-service/src/index.ts`

- [ ] **Step 6.1: Update `restate.ts`**

Read the existing `restate.ts` first. The structure is:
- A top-level `checkoutSagaRunHandler(ctx, req)` async function
- Bound via `restate.workflow({ name: checkoutSagaDef.name, handlers: { run: checkoutSagaRunHandler } })`
- A `setupRestate(opts: RestateSetupOptions)` function that listens

Two changes:
1. Add `metrics: CanaryMetrics` field to `RestateSetupOptions`.
2. Wrap the body of `checkoutSagaRunHandler` with `await measureRestate(metrics, ...)`.

The handler currently uses a module-level scope; `metrics` needs to be available where the handler runs. The cleanest approach is a module-level `let metricsRef: CanaryMetrics | null = null;` plus a `configureMetrics(m)` setter called from `setupRestate`. This mirrors the existing `configureKafkaSend` pattern in `notification-service/src/restate.ts`.

Apply the changes:

```typescript
// Near top, after existing imports:
import { measureRestate, type CanaryMetrics } from "@canary/lib-node";

// Module-level state (mirrors notification-service's configureKafkaSend pattern):
let metricsRef: CanaryMetrics | null = null;
export function configureMetrics(m: CanaryMetrics): void {
  metricsRef = m;
}

// Update RestateSetupOptions:
export interface RestateSetupOptions {
  registerHandlers: boolean;
  port: number;
  metrics: CanaryMetrics;
}

// Wrap the handler body. Existing handler:
//   export async function checkoutSagaRunHandler(ctx, req) { ... body ... }
// becomes:
export async function checkoutSagaRunHandler(
  ctx: restate.WorkflowContext,
  req: OrderRequest,
): Promise<Order> {
  const handlerName = `${checkoutSagaDef.name}.run`;
  const body = async (): Promise<Order> => {
    // ALL existing body code unchanged — paste as-is into this nested fn.
    const isCanary = ctx.request().headers.get("x-canary") === "true";
    const orderId = ctx.key;
    return runWithCanary(isCanary, async () => {
      // ... rest of existing body unchanged ...
    });
  };
  if (!metricsRef) {
    return body();   // graceful fallback if metrics not yet configured
  }
  return measureRestate(metricsRef, handlerName, body);
}

// Update setupRestate to call configureMetrics:
export async function setupRestate(opts: RestateSetupOptions): Promise<void> {
  configureMetrics(opts.metrics);
  if (!opts.registerHandlers) {
    return;
  }
  // ... rest unchanged ...
}
```

The handler-name target uses `checkoutSagaDef.name` (whatever the def declares — typically `"CheckoutSagaStable"` or `"CheckoutSagaCanary"`) so the `target` tag value matches Java convention.

- [ ] **Step 6.2: Update `index.ts`**

Pass `metrics` into `setupRestate`:

```typescript
await setupRestate({
  registerHandlers: config.RESTATE_REGISTER_HANDLERS,
  port: config.RESTATE_HANDLER_PORT,
  metrics,
});
```

- [ ] **Step 6.3: Build + test**

Run: `pnpm --filter @canary/order-service build && pnpm --filter @canary/order-service test`
Expected: BUILD + tests pass. If existing tests call `setupRestate(...)` without `metrics`, update them. For tests that mock the handler or invoke it directly, they'll need to call `configureMetrics(...)` before invoking — or accept that `metricsRef` is null and the handler runs without measurement (the graceful-fallback path).

- [ ] **Step 6.4: Commit**

```bash
git add services/order-service/src/restate.ts services/order-service/src/index.ts services/order-service/src/__tests__/
git commit -m "feat(observability): wrap order-service Restate handler with measureRestate"
```

---

## Task 7 — Wrap notification-service Restate handler

**Files:**
- Modify: `services/notification-service/src/restate.ts`
- Modify: `services/notification-service/src/index.ts`

Same pattern as Task 6, applied to `notifyHandler` in notification-service. The notification service already has a `configureKafkaSend(fn)` pattern — add a parallel `configureMetrics(m)` and a `metrics` field on `RestateSetupOptions`.

Handler-name target: `${notificationServiceDef.name}.notify`.

- [ ] **Step 7.1 — 7.4** mirror Task 6.1 — 6.4 with the appropriate substitutions.

- [ ] **Step 7.5: Commit**

```bash
git add services/notification-service/src/restate.ts services/notification-service/src/index.ts services/notification-service/src/__tests__/
git commit -m "feat(observability): wrap notification-service Restate handler with measureRestate"
```

---

## Task 8 — Conditional fallback: manual Kafka W3C trace-context (Node)

**Skip this task if the Task 10 cluster verification shows KafkaJS auto-instrumentation already creates linked spans.**

If the verification shows no parent-child link between the Node producer span and the Node/Java consumer span:

- [ ] **Step 8.1: Add `wrapKafkaProducer` helper to `lib-node`**

Edit `platform/lib-node/src/observability/canary-kafka-metrics.ts`. Add a new exported function:

```typescript
import { context, propagation, trace } from "@opentelemetry/api";
import type { Producer, ProducerRecord, Message } from "kafkajs";

/**
 * Returns a wrapped Producer whose `send` injects W3C traceparent into each
 * message's headers. Pair with consumer-side wrapKafkaConsumer for full
 * trace continuity across the broker.
 */
export function wrapKafkaProducer(producer: Producer): Producer {
  const originalSend = producer.send.bind(producer);
  producer.send = async function (record: ProducerRecord) {
    const carrier: Record<string, string> = {};
    propagation.inject(context.active(), carrier);
    const messages: Message[] = record.messages.map((m) => ({
      ...m,
      headers: { ...(m.headers ?? {}), ...carrier },
    }));
    return originalSend({ ...record, messages });
  };
  return producer;
}
```

- [ ] **Step 8.2: Extend `wrapKafkaConsumer` to extract + restore context**

Edit the same file. In `wrapKafkaConsumer`, before invoking the user's `eachMessage`, extract trace context from message headers and run the user code inside that context:

```typescript
import { context, propagation } from "@opentelemetry/api";

// inside wrapKafkaConsumer's wrapped eachMessage, replace the existing
// `try { await userEachMessage(payload); ... } catch ...` body with:
const carrier: Record<string, string> = {};
for (const [k, v] of Object.entries(payload.message.headers ?? {})) {
  if (v != null) carrier[k] = String(v);
}
const parent = propagation.extract(context.active(), carrier);
return context.with(parent, async () => {
  const startNs = process.hrtime.bigint();
  try {
    await userEachMessage(payload);
    metrics.recordKafka(payload.topic, "success", Number(process.hrtime.bigint() - startNs) / 1e9);
  } catch (err) {
    metrics.recordKafka(payload.topic, "server_error", Number(process.hrtime.bigint() - startNs) / 1e9);
    throw err;
  }
});
```

- [ ] **Step 8.3: Update both Node services to use `wrapKafkaProducer` on their producers**

Edit `services/{order,notification}-service/src/kafka.ts`. Where the producer is created and its `send` method is wrapped/exposed, apply `wrapKafkaProducer(producer)` after the producer connects.

- [ ] **Step 8.4: Build + test**

Run: `pnpm --filter @canary/lib-node build && pnpm --filter @canary/lib-node test && pnpm --filter @canary/order-service test && pnpm --filter @canary/notification-service test`
Expected: PASS.

- [ ] **Step 8.5: Commit**

```bash
git add platform/lib-node/src/observability/canary-kafka-metrics.ts \
        services/order-service/src/kafka.ts services/notification-service/src/kafka.ts
git commit -m "fix(observability): manually inject + extract W3C traceparent on Kafka (Node)"
```

---

## Task 9 — Conditional fallback: manual Restate W3C trace-context

**Skip this task if the Task 10 cluster verification shows the Restate Java SDK 2.7.0 + Node SDK 1.14.2 already propagate `traceparent` across `ctx.call(...)` / `ctx.send(...)`.**

If verification shows the parent-child link is broken at Restate hops:

- [ ] **Step 9.1: Java — extend `XCanaryRestateClientCustomizer` to also stamp `traceparent`**

Edit `platform/lib-java/src/main/java/com/canary/platform/lib/XCanaryRestateClientCustomizer.java`. After the existing `x-canary` header injection, add:

```java
io.opentelemetry.api.trace.Span span = io.opentelemetry.api.trace.Span.current();
if (span.getSpanContext().isValid()) {
    String traceparent = String.format("00-%s-%s-%02x",
        span.getSpanContext().getTraceId(),
        span.getSpanContext().getSpanId(),
        span.getSpanContext().getTraceFlags().asByte());
    headers.put("traceparent", traceparent);
}
```

(Keep the existing `x-canary` logic unchanged.)

- [ ] **Step 9.2: Node — extend `applyXCanaryToRestateOptions` similarly**

Edit `platform/lib-node/src/x-canary-restate.ts`. After the existing `x-canary` header injection:

```typescript
import { context, propagation } from "@opentelemetry/api";
const carrier: Record<string, string> = {};
propagation.inject(context.active(), carrier);
for (const [k, v] of Object.entries(carrier)) {
  if (headers[k] === undefined) headers[k] = v;
}
```

- [ ] **Step 9.3: Build + test on both sides**

`./gradlew :platform:lib-java:test && pnpm --filter @canary/lib-node test`. Expected: PASS.

- [ ] **Step 9.4: Commit**

```bash
git add platform/lib-java/src/main/java/com/canary/platform/lib/XCanaryRestateClientCustomizer.java \
        platform/lib-node/src/x-canary-restate.ts
git commit -m "fix(observability): manually inject W3C traceparent on Restate inter-handler calls"
```

---

## Task 10 — End-to-end cluster verification (deferred to user)

This task validates the trace propagation in a real cluster. It also gates Tasks 8 + 9 (only do them if this verification fails).

Skipped during plan execution; ran by the user.

- [ ] **Step 10.1: Apply manifests + bring up cluster**

Run: `make all` (and re-apply the Restate StatefulSet to pick up the new env: `kubectl apply -f deploy/kind/restate/statefulset.yaml`). Honor `feedback_e2e_inpod_probes.md`.

- [ ] **Step 10.2: Drive a request that crosses HTTP → Kafka → Restate → Kafka**

Use existing E2E scenarios from Phase 1.5 (`tests/e2e/`). Pick one that exercises the full saga end-to-end (e.g. `r1-r5-restate-saga.test.ts`).

- [ ] **Step 10.3: Open Jaeger and search for the trace**

```bash
kubectl port-forward -n istio-system svc/tracing 16686:80 &
sleep 3
curl -s 'http://localhost:16686/api/traces?service=order-service&tag=canary.lane%3Acanary&limit=5' | jq '.data[0].spans | length'
```

Expected: ≥ 5 spans (HTTP → Kafka producer → Kafka consumer → Restate handlers).

- [ ] **Step 10.4: Verify Kafka span linkage**

For one of the returned traces, check that:
- A `kafka send` span (from a producer) has a child `kafka receive` span (in the consuming service).
- The parent-child link is via `references[].traceID == parent.traceID`.

If this link is missing → Task 8 (fallback) is needed. Apply Task 8, redeploy, re-verify.

- [ ] **Step 10.5: Verify Restate span linkage**

Check that:
- A Restate handler span (e.g. `CheckoutSagaCanary.run`) has a child span for the Restate handler it calls (e.g. `PaymentVOCanary.charge`).
- The parent-child link exists.

If missing → Task 9 is needed. Apply Task 9, redeploy, re-verify.

- [ ] **Step 10.6: Verify per-handler metrics in Prometheus**

```bash
kubectl port-forward -n istio-system svc/prometheus 9090:9090 &
sleep 3
curl -s 'http://localhost:9090/api/v1/query?query=canary_request_total{substrate="restate"}' | jq '.data.result | length'
```

Expected: ≥ 1 row per handler invoked (≥ 12 if every handler in every service was hit by the saga).

---

## Self-Review

**Spec coverage:**
- Spec §"Restate handler measure() wiring" — Tasks 3 (payment), 4 (inventory), 5 (audit), 6 (order Node), 7 (notification Node). ✓
- Spec §"T2 Kafka trace-context — Java details" — Task 2. ✓
- Spec §"T2 Kafka trace-context — Node details" verification + fallback — Task 10.4 (verify) + Task 8 (conditional fallback). ✓
- Spec §"T3 Restate trace-context — Restate StatefulSet env" — Task 1. ✓
- Spec §"T3 Restate trace-context — Java SDK propagation verification" — Task 10.5 (verify) + Task 9 (conditional fallback). ✓
- Spec §"T3 Restate trace-context — Node SDK propagation verification" — Task 10.5 (verify) + Task 9 (conditional fallback Node-side). ✓
- Spec note: shadow-mismatch wiring deliberately out of scope (see spec §Non-goals). ✓

**Placeholders:** No "TBD", "TODO", "fill in details". Tasks 8 and 9 are explicitly conditional with "skip if verification passes".

**Type consistency:**
- `CanaryRestateMeter.measure(handlerName, body)` 2-arg signature consistent throughout (matches the helper landed in 5.a).
- `measureRestate(metrics, handlerName, body)` 3-arg signature consistent (matches helper from 5.a-node).
- Handler-name target convention `<DefName>.<methodName>` consistent across Java + Node.
- `ConsumerRecord<K, V>`, `ConcurrentKafkaListenerContainerFactory<?, ?>` types unchanged from existing code.

**Open coverage gap accepted at planning time:** Tests for the wrapped handlers don't directly assert that the meter was called — that would require either a `MeterRegistry`-bound test for each handler, or trusting the existing `CanaryRestateMeter` unit tests + the existing handler tests. Accepted: the meter has its own unit tests; the wrapping is mechanical.

---

## Out of scope for 5.b (handed forward)

- Recording rules, alerts, alertmanager, alert-sink — Phase 5.c
- Grafana dashboards, runbooks, E2E test additions — Phase 5.d
- True shadow-read implementation + `recordShadowMismatch` wiring — no current sub-phase planned

---

## Plan complete

Plan complete and saved to `docs/superpowers/plans/2026-05-11-canary-release-phase-5-b-trace-propagation.md`.
