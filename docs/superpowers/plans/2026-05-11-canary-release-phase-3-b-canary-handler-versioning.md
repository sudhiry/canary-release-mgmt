# Canary Release Phase 3.b — Canary Handler Versioning Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Lift `RESTATE_REGISTER_HANDLERS=false` on canary pods. Stable and canary register as distinct Restate service names (`*Stable` / `*Canary`); HTTP controller maps `x-canary` header to a service-name suffix on Restate Ingress URLs. Per-subset K8s Services let Restate dispatch to one variant only.

**Architecture:** β (header-routed on top of Restate). Each pod registers exactly one variant of every handler. Saga downstream clients are bound to matching-variant defs at boot. Phase 1 (HTTP via Istio) and Phase 2 (Kafka via groups) routing are untouched.

**Tech Stack:** Restate Server 1.6.2, Java SDK 2.7.0, Node SDK 1.14.2, Spring Boot 4 + spring-kafka 4.0.4, kafkajs 2.2.4, vitest, gradle, helm, kind.

**Spec:** `docs/superpowers/specs/2026-05-11-canary-release-phase-3-b-canary-handler-versioning-design.md`

**Behavioral tweaks per canary handler (observable in saga response):**
- `CheckoutSaga` canary: produces `Order.auditTrail = ["saga@canary","reservation@canary","payment@canary","notification@canary"]`
- `ReservationWorkflow` canary: `Reservation.bufferUnits = 1` (stable: 0)
- `PaymentVO` canary: `Charge.amount = (req.amount * 99) / 100` (stable: `req.amount`)
- `NotificationService` canary: appends `[via canary notifier]` to delivered message; `NotifyResult.version = "canary"`

**Failure modes covered (from spec § Error handling):** F1 registration race, F2 cold canary, F3 in-flight teardown, F4 saga refactor regression, F5 pod label drift, F6 controller routing bug.

---

## Task ordering

T1-T3: Restate definitions (Java + Node) — foundational types.
T4-T7: Service implementations (inventory → payment → order → notification).
T8: Helm chart (per-subset Services + variant-aware registration).
T9-T11: E2E tests (extend R1-R5, add R6 isolation, add R7 lifecycle).

Each task self-contained: write failing test, implement, run tests, commit.

---

### Task 1: Split `restate-defs-java` ReservationWorkflow into Stable/Canary variants

**Files:**
- Modify: `platform/restate-defs-java/src/main/java/com/canary/restate/inventory/Reservation.java`
- Delete: `platform/restate-defs-java/src/main/java/com/canary/restate/inventory/ReservationWorkflow.java`
- Create: `platform/restate-defs-java/src/main/java/com/canary/restate/inventory/ReservationWorkflowStable.java`
- Create: `platform/restate-defs-java/src/main/java/com/canary/restate/inventory/ReservationWorkflowCanary.java`

- [ ] **Step 1: Add `bufferUnits` field to Reservation record**

```java
// platform/restate-defs-java/src/main/java/com/canary/restate/inventory/Reservation.java
package com.canary.restate.inventory;

public record Reservation(
    String id,
    String sku,
    int quantity,
    String orderId,
    String status,
    int bufferUnits   // NEW: 0 stable, 1 canary; derived at response-time, not persisted
) {}
```

- [ ] **Step 2: Create ReservationWorkflowStable**

```java
// platform/restate-defs-java/src/main/java/com/canary/restate/inventory/ReservationWorkflowStable.java
package com.canary.restate.inventory;

import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.Shared;
import dev.restate.sdk.annotation.Workflow;

/**
 * Stable variant of the inventory reservation workflow. Registered under the
 * Restate service name "ReservationWorkflowStable" by stable-tagged inventory
 * pods. Canary pods register {@link ReservationWorkflowCanary}.
 *
 * <p>Lifecycle is identical to the canary variant; only the registered service
 * name differs. Implementations delegate to {@code ReservationWorkflowCore}.
 */
@Workflow(name = "ReservationWorkflowStable")
public abstract class ReservationWorkflowStable {
    @Handler
    public abstract Reservation run(ReservationRequest req);

    @Shared
    public abstract Reservation confirm();   // returns confirmed Reservation; was void in Phase 3.a

    @Shared
    public abstract void release();
}
```

- [ ] **Step 3: Create ReservationWorkflowCanary**

```java
// platform/restate-defs-java/src/main/java/com/canary/restate/inventory/ReservationWorkflowCanary.java
package com.canary.restate.inventory;

import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.Shared;
import dev.restate.sdk.annotation.Workflow;

/**
 * Canary variant. Same handler signatures as the stable variant; differs only
 * in the registered service name. Canary impl returns Reservation with
 * bufferUnits=1 to signal the canary tweak.
 */
@Workflow(name = "ReservationWorkflowCanary")
public abstract class ReservationWorkflowCanary {
    @Handler
    public abstract Reservation run(ReservationRequest req);

    @Shared
    public abstract Reservation confirm();

    @Shared
    public abstract void release();
}
```

- [ ] **Step 4: Delete the old ReservationWorkflow.java**

```bash
rm platform/restate-defs-java/src/main/java/com/canary/restate/inventory/ReservationWorkflow.java
```

This will break inventory-service compilation; downstream tasks fix the impl.

- [ ] **Step 5: Build the defs module to verify it compiles standalone**

Run: `./gradlew :platform:restate-defs-java:build`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 6: Commit**

```bash
git add platform/restate-defs-java/src/main/java/com/canary/restate/inventory/
git commit -m "feat(restate-defs-java): split ReservationWorkflow into Stable/Canary; bufferUnits + confirm() return"
```

---

### Task 2: Split `restate-defs-java` PaymentVO into Stable/Canary variants

**Files:**
- Delete: `platform/restate-defs-java/src/main/java/com/canary/restate/payment/PaymentVO.java`
- Create: `platform/restate-defs-java/src/main/java/com/canary/restate/payment/PaymentVOStable.java`
- Create: `platform/restate-defs-java/src/main/java/com/canary/restate/payment/PaymentVOCanary.java`
- (Charge.java unchanged — `Charge.amount` is reused as the actually-charged amount; canary returns `amount = (req.amount * 99) / 100`)

- [ ] **Step 1: Create PaymentVOStable**

```java
// platform/restate-defs-java/src/main/java/com/canary/restate/payment/PaymentVOStable.java
package com.canary.restate.payment;

import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.VirtualObject;

/**
 * Stable variant of payment VO. Registered under "PaymentVOStable" by
 * stable-tagged payment pods. Canary pods register {@link PaymentVOCanary}.
 *
 * <p>Behavior identical to canary except {@code charge.amount} reflects the full
 * requested amount (canary applies 1% discount).
 */
@VirtualObject(name = "PaymentVOStable")
public abstract class PaymentVOStable {
    @Handler
    public abstract Charge charge(ChargeRequest req);

    @Handler
    public abstract Charge refund(ChargeRequest req);
}
```

- [ ] **Step 2: Create PaymentVOCanary**

```java
// platform/restate-defs-java/src/main/java/com/canary/restate/payment/PaymentVOCanary.java
package com.canary.restate.payment;

import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.VirtualObject;

@VirtualObject(name = "PaymentVOCanary")
public abstract class PaymentVOCanary {
    @Handler
    public abstract Charge charge(ChargeRequest req);

    @Handler
    public abstract Charge refund(ChargeRequest req);
}
```

- [ ] **Step 3: Delete the old PaymentVO.java**

```bash
rm platform/restate-defs-java/src/main/java/com/canary/restate/payment/PaymentVO.java
```

This will break payment-service compilation; Task 5 fixes it.

- [ ] **Step 4: Build the defs module**

Run: `./gradlew :platform:restate-defs-java:build`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 5: Commit**

```bash
git add platform/restate-defs-java/src/main/java/com/canary/restate/payment/
git commit -m "feat(restate-defs-java): split PaymentVO into Stable/Canary variants"
```

---

### Task 3: Add variant defs and Order.auditTrail to `restate-defs-node`

**Files:**
- Modify: `platform/restate-defs-node/src/index.ts`

- [ ] **Step 1: Replace single defs with variant pairs and update Order/NotifyResult**

Replace the file contents:

```typescript
// platform/restate-defs-node/src/index.ts
import * as restate from "@restatedev/restate-sdk";

// ----- audit -----
export interface AuditEvent {
  aggregate: string;
  id: string;
  action: string;
  correlationId?: string;
}

export type AuditQueryServiceMethods = {
  append(event: AuditEvent): Promise<void>;
  byAggregate(aggregateId: string): Promise<AuditEvent[]>;
};

// audit is not subset-forked; one definition.
export const auditQueryServiceDef = {
  name: "AuditQueryService",
} as restate.ServiceDefinitionFrom<AuditQueryServiceMethods>;

// ----- payment -----
export interface ChargeRequest {
  orderId: string;
  amount: number;
}

export interface Charge {
  id: string;
  orderId: string;
  amount: number;     // canary: (req.amount * 99) / 100; stable: req.amount
  status: string;
}

export type PaymentVOMethods = {
  charge(req: ChargeRequest): Promise<Charge>;
  refund(req: ChargeRequest): Promise<Charge>;
};

export const paymentVOStableDef = {
  name: "PaymentVOStable",
} as restate.VirtualObjectDefinitionFrom<PaymentVOMethods>;

export const paymentVOCanaryDef = {
  name: "PaymentVOCanary",
} as restate.VirtualObjectDefinitionFrom<PaymentVOMethods>;

// ----- inventory -----
export interface ReservationRequest {
  sku: string;
  quantity: number;
  orderId: string;
}

export interface Reservation {
  id: string;
  sku: string;
  quantity: number;
  orderId: string;
  status: string;
  bufferUnits: number;   // 0 stable, 1 canary
}

export interface AvailabilityResponse {
  sku: string;
  available: number;
}

export type ReservationWorkflowMethods = {
  run(req: ReservationRequest): Promise<Reservation>;
  confirm(): Promise<Reservation>;   // was Promise<void>; now returns confirmed Reservation
  release(): Promise<void>;
};

export const reservationWorkflowStableDef = {
  name: "ReservationWorkflowStable",
} as restate.WorkflowDefinitionFrom<ReservationWorkflowMethods>;

export const reservationWorkflowCanaryDef = {
  name: "ReservationWorkflowCanary",
} as restate.WorkflowDefinitionFrom<ReservationWorkflowMethods>;

// ----- notification -----
export interface NotifyRequest {
  userId: string;
  message: string;
  orderId: string;
}

export interface NotifyResult {
  delivered: boolean;
  version: "stable" | "canary";
  deliveredMessage: string;   // canary appends "[via canary notifier]"
}

export type NotificationServiceMethods = {
  notify(req: NotifyRequest): Promise<NotifyResult>;
};

export const notificationServiceStableDef = {
  name: "NotificationServiceStable",
} as restate.ServiceDefinitionFrom<NotificationServiceMethods>;

export const notificationServiceCanaryDef = {
  name: "NotificationServiceCanary",
} as restate.ServiceDefinitionFrom<NotificationServiceMethods>;

// ----- order -----
export interface OrderRequest {
  userId: string;
  sku: string;
  quantity: number;
  amount: number;
}

export interface Order {
  id: string;
  userId: string;
  sku: string;
  quantity: number;
  amount: number;
  status: string;
  auditTrail: string[];   // NEW: per-hop "<svc>@<variant>" entries
}

export type CheckoutSagaMethods = {
  run(req: OrderRequest): Promise<Order>;
};

export const checkoutSagaStableDef = {
  name: "CheckoutSagaStable",
} as restate.WorkflowDefinitionFrom<CheckoutSagaMethods>;

export const checkoutSagaCanaryDef = {
  name: "CheckoutSagaCanary",
} as restate.WorkflowDefinitionFrom<CheckoutSagaMethods>;
```

- [ ] **Step 2: Build defs-node**

Run: `npm --workspace=@canary/restate-defs-node run build`
Expected: tsc emits without errors.

- [ ] **Step 3: Commit**

```bash
git add platform/restate-defs-node/src/index.ts
git commit -m "feat(restate-defs-node): variant defs + auditTrail + NotifyResult + Reservation.bufferUnits"
```

---

### Task 4: inventory-service — Core + Stable/Canary impl delegates + Spring wiring

**Files:**
- Rename: `services/inventory-service/src/main/java/com/canary/inventory/handler/ReservationWorkflowImpl.java` → `ReservationWorkflowCore.java` (and refactor)
- Create: `services/inventory-service/src/main/java/com/canary/inventory/handler/ReservationWorkflowImplStable.java`
- Create: `services/inventory-service/src/main/java/com/canary/inventory/handler/ReservationWorkflowImplCanary.java`
- Modify: `services/inventory-service/src/main/java/com/canary/inventory/config/RestateEndpointConfig.java`
- Test: `services/inventory-service/src/test/java/com/canary/inventory/handler/ReservationWorkflowCoreTest.java`
- Modify test: `services/inventory-service/src/test/java/com/canary/inventory/config/RestateEndpointGatingTest.java`

- [ ] **Step 1: Refactor ReservationWorkflowImpl → ReservationWorkflowCore**

Read the existing `ReservationWorkflowImpl.java`. Rename the file and class to `ReservationWorkflowCore`. Add a constructor parameter `boolean isCanary`; store as a private final field. **Do not extend any abstract class** — the Core is the shared logic, not a Restate handler binding.

In every place where the Core constructs and returns a `Reservation`, set `bufferUnits = isCanary ? 1 : 0`. Specifically:
- The handler that returns Reservation from `run(...)` — set bufferUnits on the returned object.
- The new `confirm()` (which now returns Reservation) — return the current reservation state with the correct bufferUnits.
- Wherever Reservation is constructed for state persistence, store with `bufferUnits = 0` (state is variant-agnostic; bufferUnits is response-only). When reading from state and returning, override the `bufferUnits` field with the live `isCanary` flag before returning.

Helper to centralize the variant-stamp logic:

```java
private Reservation withVariant(Reservation r) {
    return new Reservation(r.id(), r.sku(), r.quantity(), r.orderId(), r.status(),
                            isCanary ? 1 : 0);
}
```

Use `withVariant(...)` on every return path that exits the Core.

The `confirm()` method change (was void, now returns Reservation): after the awakeable resolves with "confirm", the method must read the current reservation state and return it via `withVariant(...)`. Reuse the existing transition logic; just add the read+return at the end.

- [ ] **Step 2: Create ReservationWorkflowImplStable (thin delegate)**

```java
// services/inventory-service/src/main/java/com/canary/inventory/handler/ReservationWorkflowImplStable.java
package com.canary.inventory.handler;

import com.canary.inventory.store.ReservationStore;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import com.canary.restate.inventory.Reservation;
import com.canary.restate.inventory.ReservationRequest;
import com.canary.restate.inventory.ReservationWorkflowStable;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Restate-binding subclass for the stable variant. Delegates all handlers to a
 * shared {@link ReservationWorkflowCore} instance constructed with isCanary=false.
 */
public class ReservationWorkflowImplStable extends ReservationWorkflowStable {
    private final ReservationWorkflowCore core;

    public ReservationWorkflowImplStable(ReservationStore store,
                                          XCanaryRestateClientCustomizer canary,
                                          KafkaTemplate<String, String> kafkaTemplate,
                                          ObjectMapper objectMapper) {
        this.core = new ReservationWorkflowCore(store, canary, kafkaTemplate, objectMapper, false);
    }

    @Override
    public Reservation run(ReservationRequest req) { return core.run(req); }

    @Override
    public Reservation confirm() { return core.confirm(); }

    @Override
    public void release() { core.release(); }
}
```

- [ ] **Step 3: Create ReservationWorkflowImplCanary (mirror)**

```java
// services/inventory-service/src/main/java/com/canary/inventory/handler/ReservationWorkflowImplCanary.java
package com.canary.inventory.handler;

import com.canary.inventory.store.ReservationStore;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import com.canary.restate.inventory.Reservation;
import com.canary.restate.inventory.ReservationRequest;
import com.canary.restate.inventory.ReservationWorkflowCanary;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;

public class ReservationWorkflowImplCanary extends ReservationWorkflowCanary {
    private final ReservationWorkflowCore core;

    public ReservationWorkflowImplCanary(ReservationStore store,
                                          XCanaryRestateClientCustomizer canary,
                                          KafkaTemplate<String, String> kafkaTemplate,
                                          ObjectMapper objectMapper) {
        this.core = new ReservationWorkflowCore(store, canary, kafkaTemplate, objectMapper, true);
    }

    @Override
    public Reservation run(ReservationRequest req) { return core.run(req); }

    @Override
    public Reservation confirm() { return core.confirm(); }

    @Override
    public void release() { core.release(); }
}
```

- [ ] **Step 4: Update RestateEndpointConfig for variant-conditional wiring**

Replace `RestateEndpointConfig.java`:

```java
package com.canary.inventory.config;

import com.canary.inventory.handler.ReservationWorkflowImplStable;
import com.canary.inventory.handler.ReservationWorkflowImplCanary;
import com.canary.inventory.store.ReservationStore;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.restate.sdk.endpoint.Endpoint;
import dev.restate.sdk.http.vertx.RestateHttpServer;
import io.vertx.core.http.HttpServer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Starts the Restate HTTP endpoint. Variant is determined by {@code app.version}:
 * "stable" wires {@link ReservationWorkflowImplStable}; "canary" wires
 * {@link ReservationWorkflowImplCanary}. Gated overall by
 * {@code app.restate.register-handlers} (default true).
 */
@Configuration
@ConditionalOnProperty(
    name = "app.restate.register-handlers",
    havingValue = "true",
    matchIfMissing = true
)
public class RestateEndpointConfig {

    private final int port;
    private final Object handler;   // Object so either Stable or Canary impl works
    private HttpServer server;

    public RestateEndpointConfig(@Value("${app.restate.handler.port}") int port,
                                 @org.springframework.beans.factory.annotation.Autowired(required = false)
                                     ReservationWorkflowImplStable stableHandler,
                                 @org.springframework.beans.factory.annotation.Autowired(required = false)
                                     ReservationWorkflowImplCanary canaryHandler) {
        this.port = port;
        if (stableHandler != null && canaryHandler != null) {
            throw new IllegalStateException(
                "Both stable and canary impls present; check app.version configuration");
        }
        if (stableHandler != null) this.handler = stableHandler;
        else if (canaryHandler != null) this.handler = canaryHandler;
        else throw new IllegalStateException(
            "No reservation handler bean present; expected app.version=stable|canary");
    }

    @Bean
    @ConditionalOnProperty(name = "app.version", havingValue = "stable", matchIfMissing = true)
    public static ReservationWorkflowImplStable reservationWorkflowImplStable(
            ReservationStore store,
            XCanaryRestateClientCustomizer canary,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper) {
        return new ReservationWorkflowImplStable(store, canary, kafkaTemplate, objectMapper);
    }

    @Bean
    @ConditionalOnProperty(name = "app.version", havingValue = "canary")
    public static ReservationWorkflowImplCanary reservationWorkflowImplCanary(
            ReservationStore store,
            XCanaryRestateClientCustomizer canary,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper) {
        return new ReservationWorkflowImplCanary(store, canary, kafkaTemplate, objectMapper);
    }

    @PostConstruct
    void start() throws Exception {
        Endpoint endpoint = Endpoint.builder().bind(handler).build();
        server = RestateHttpServer.fromEndpoint(endpoint);
        server.listen(port).toCompletionStage().toCompletableFuture().get();
    }

    @PreDestroy
    void stop() throws Exception {
        if (server != null) {
            server.close().toCompletionStage().toCompletableFuture().get();
        }
    }
}
```

The dual-`@Autowired(required=false)` constructor + IllegalStateException provides the F5 (env mismatch) defensive check.

Add `app.version=stable` to `services/inventory-service/src/main/resources/application.yml` if not already present (matchIfMissing covers default).

- [ ] **Step 5: Write parameterized unit test for ReservationWorkflowCore**

Replace `ReservationWorkflowImplTest.java` with `ReservationWorkflowCoreTest.java`:

```java
// services/inventory-service/src/test/java/com/canary/inventory/handler/ReservationWorkflowCoreTest.java
package com.canary.inventory.handler;

// (use existing imports + test scaffolding from ReservationWorkflowImplTest)

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.assertThat;

class ReservationWorkflowCoreTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void runReturnsReservationWithMatchingBufferUnits(boolean isCanary) {
        ReservationWorkflowCore core = newCore(isCanary);   // helper using existing test scaffolding
        // simulate run() with mocked WorkflowContext returning awakeable resolved with "confirm"
        // ... (port the existing ReservationWorkflowImplTest body, parameterizing on isCanary)
        Reservation r = core.run(new ReservationRequest("SKU1", 2, "ord-1"));
        assertThat(r.bufferUnits()).isEqualTo(isCanary ? 1 : 0);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void confirmReturnsConfirmedReservationWithMatchingBufferUnits(boolean isCanary) {
        ReservationWorkflowCore core = newCore(isCanary);
        // simulate confirm flow
        Reservation r = core.confirm();
        assertThat(r.status()).isEqualTo("confirmed");
        assertThat(r.bufferUnits()).isEqualTo(isCanary ? 1 : 0);
    }

    // Port the existing release / expiry / awakeable-race assertions; they should
    // pass identically for both isCanary values (release returns void; bufferUnits
    // only observable on run()/confirm() return paths).
}
```

Run: `./gradlew :services:inventory-service:test --tests ReservationWorkflowCoreTest`
Expected: PASS for both isCanary=false and =true.

- [ ] **Step 6: Update RestateEndpointGatingTest**

Add three new test cases:

```java
// services/inventory-service/src/test/java/com/canary/inventory/config/RestateEndpointGatingTest.java
@Test
@org.springframework.test.context.TestPropertySource(properties = {
    "app.version=stable", "app.restate.register-handlers=true"
})
void wiresStableImplWhenVersionIsStable() {
    // assert ApplicationContext contains ReservationWorkflowImplStable
    // assert ApplicationContext does NOT contain ReservationWorkflowImplCanary
}

@Test
@org.springframework.test.context.TestPropertySource(properties = {
    "app.version=canary", "app.restate.register-handlers=true"
})
void wiresCanaryImplWhenVersionIsCanary() {
    // mirror
}

@Test
void rejectsUnknownVersion() {
    // assert context startup fails with IllegalStateException for app.version=banana
}
```

Run: `./gradlew :services:inventory-service:test`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 7: Commit**

```bash
git add services/inventory-service/
git commit -m "feat(inventory-service): variant-aware ReservationWorkflow Core + Stable/Canary impls"
```

---

### Task 5: payment-service — Core + Stable/Canary impl delegates + Spring wiring

Mirror of Task 4. Same shape, different domain.

**Files:**
- Rename: `services/payment-service/src/main/java/com/canary/payment/handler/PaymentVOImpl.java` → `PaymentVOCore.java` (and refactor to take `boolean isCanary`)
- Create: `services/payment-service/src/main/java/com/canary/payment/handler/PaymentVOImplStable.java`
- Create: `services/payment-service/src/main/java/com/canary/payment/handler/PaymentVOImplCanary.java`
- Modify: `services/payment-service/src/main/java/com/canary/payment/config/RestateEndpointConfig.java` (assume parallel structure exists; mirror Task 4 Step 4)
- Test: `services/payment-service/src/test/java/com/canary/payment/handler/PaymentVOCoreTest.java`

- [ ] **Step 1: Refactor PaymentVOImpl → PaymentVOCore**

Add constructor `boolean isCanary` field. In `charge()`, change the line:

```java
// before:
Charge charge = new Charge(UUID.randomUUID().toString(), req.orderId(), req.amount(), "succeeded");

// after:
long actualAmount = isCanary ? (req.amount() * 99L) / 100L : req.amount();
Charge charge = new Charge(UUID.randomUUID().toString(), req.orderId(), actualAmount, "succeeded");
```

`refund()` reads `prior` from state; the prior charge's `amount` already reflects whatever was charged (canary 99%, stable 100%). No isCanary branch in refund.

The Core class **does not extend any abstract**. It's the shared logic, instantiated by the variant-impl classes.

- [ ] **Step 2: Create PaymentVOImplStable**

```java
package com.canary.payment.handler;

import com.canary.payment.store.ChargeStore;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import com.canary.restate.payment.Charge;
import com.canary.restate.payment.ChargeRequest;
import com.canary.restate.payment.PaymentVOStable;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;

public class PaymentVOImplStable extends PaymentVOStable {
    private final PaymentVOCore core;

    public PaymentVOImplStable(ChargeStore store, XCanaryRestateClientCustomizer canary,
                                KafkaTemplate<String, String> kafkaTemplate,
                                ObjectMapper objectMapper) {
        this.core = new PaymentVOCore(store, canary, kafkaTemplate, objectMapper, false);
    }

    @Override
    public Charge charge(ChargeRequest req) { return core.charge(req); }

    @Override
    public Charge refund(ChargeRequest req) { return core.refund(req); }
}
```

- [ ] **Step 3: Create PaymentVOImplCanary**

Mirror with `isCanary=true` and `extends PaymentVOCanary`.

- [ ] **Step 4: Update payment-service RestateEndpointConfig**

Mirror Task 4 Step 4. If payment-service doesn't have a RestateEndpointConfig today, create one following the inventory pattern (read the existing payment service config to see how Restate endpoint is wired today; refactor to variant-conditional).

- [ ] **Step 5: Write parameterized PaymentVOCoreTest**

```java
package com.canary.payment.handler;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import static org.assertj.core.api.Assertions.assertThat;

class PaymentVOCoreTest {

    @ParameterizedTest
    @CsvSource({
        "false, 1000, 1000",
        "true,  1000, 990",
        "true,  99,    98",     // integer truncation: (99 * 99) / 100 = 98
        "false, 99,    99",
        "true,  101,  99",      // (101 * 99) / 100 = 99
    })
    void chargeAppliesCanaryDiscount(boolean isCanary, long requested, long expectedCharged) {
        PaymentVOCore core = newCore(isCanary);
        Charge result = core.charge(new ChargeRequest("ord-1", requested));
        assertThat(result.amount()).isEqualTo(expectedCharged);
        assertThat(result.status()).isEqualTo("succeeded");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void refundFlipsStatusToRefunded(boolean isCanary) {
        // port existing refund tests; refund preserves prior.amount; no canary effect
    }

    // Port other existing PaymentVOImpl tests, parameterizing where applicable.
}
```

Run: `./gradlew :services:payment-service:test`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 6: Commit**

```bash
git add services/payment-service/ platform/restate-defs-java/
git commit -m "feat(payment-service): variant-aware PaymentVO Core + Stable/Canary impls"
```

---

### Task 6: order-service — variant-aware saga + controller header routing

**Files:**
- Modify: `services/order-service/src/restate.ts`
- Modify: `services/order-service/src/http.ts`
- Test create: `services/order-service/src/__tests__/saga-variant-binding.test.ts`
- Modify test: `services/order-service/src/__tests__/http.test.ts` (or add a new test if missing)

- [ ] **Step 1: Rewrite restate.ts to bind variant at module load**

Replace `services/order-service/src/restate.ts`:

```typescript
import * as restate from "@restatedev/restate-sdk";
import { runWithCanary, applyXCanaryToRestateOptions } from "@canary/lib-node";
import {
  checkoutSagaStableDef,
  checkoutSagaCanaryDef,
  paymentVOStableDef,
  paymentVOCanaryDef,
  reservationWorkflowStableDef,
  reservationWorkflowCanaryDef,
  notificationServiceStableDef,
  notificationServiceCanaryDef,
  type Order,
  type OrderRequest,
} from "@canary/restate-defs-node";

export interface RestateSetupOptions {
  registerHandlers: boolean;
  port: number;
}

const MY_VARIANT: "stable" | "canary" =
  process.env.VERSION === "canary" ? "canary" : "stable";

// Pick the matching set of defs at module load. Saga is locked to its own
// variant for all downstream calls — never re-evaluates per-request.
const checkoutSagaDef =
  MY_VARIANT === "canary" ? checkoutSagaCanaryDef : checkoutSagaStableDef;
const paymentVODef =
  MY_VARIANT === "canary" ? paymentVOCanaryDef : paymentVOStableDef;
const reservationWorkflowDef =
  MY_VARIANT === "canary" ? reservationWorkflowCanaryDef : reservationWorkflowStableDef;
const notificationServiceDef =
  MY_VARIANT === "canary" ? notificationServiceCanaryDef : notificationServiceStableDef;

export async function checkoutSagaRunHandler(
  ctx: restate.WorkflowContext,
  req: OrderRequest,
): Promise<Order> {
  const isCanary = ctx.request().headers.get("x-canary") === "true";
  const orderId = ctx.key;

  return runWithCanary(isCanary, async () => {
    const auditTrail: string[] = [
      `saga@${MY_VARIANT}`,
      `reservation@${MY_VARIANT}`,   // by-construction trust
    ];

    const order: Order = {
      id: orderId,
      userId: req.userId,
      sku: req.sku,
      quantity: req.quantity,
      amount: req.amount,
      status: "pending",
      auditTrail,
    };

    const reservationSendClient = ctx.workflowSendClient(reservationWorkflowDef, orderId);
    const reservationClient = ctx.workflowClient(reservationWorkflowDef, orderId);
    const paymentClient = ctx.objectClient(paymentVODef, orderId);
    const notificationClient = ctx.serviceClient(notificationServiceDef);

    // Step 1: reserve (fire-and-forget; parks on awakeable+timer)
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      await (reservationSendClient as any).run(
        { sku: req.sku, quantity: req.quantity, orderId },
        restate.rpc.sendOpts(applyXCanaryToRestateOptions({})),
      );
    } catch (e) {
      if (e instanceof restate.TerminalError) {
        return { ...order, status: "failed" };
      }
      throw e;
    }

    // Step 2: charge — observe canary tweak via charge.amount math
    let chargedAmount: number;
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const charge = await (paymentClient as any).charge(
        { orderId, amount: req.amount },
        restate.rpc.opts(applyXCanaryToRestateOptions({})),
      );
      chargedAmount = charge.amount;
      const paymentVariant = chargedAmount === req.amount ? "stable" : "canary";
      auditTrail.push(`payment@${paymentVariant}`);
    } catch (e) {
      if (e instanceof restate.TerminalError) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        await (reservationClient as any).release(
          restate.rpc.opts(applyXCanaryToRestateOptions({})),
        );
        return { ...order, status: "failed" };
      }
      throw e;
    }

    // Step 3: confirm — now returns Reservation; bufferUnits attests the variant
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const confirmed = await (reservationClient as any).confirm(
        restate.rpc.opts(applyXCanaryToRestateOptions({})),
      );
      // confirmed.bufferUnits should match MY_VARIANT (sanity, not enforced here)
      void confirmed;
    } catch (e) {
      if (e instanceof restate.TerminalError) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        await (paymentClient as any).refund(
          { orderId, amount: req.amount },
          restate.rpc.opts(applyXCanaryToRestateOptions({})),
        );
        return { ...order, status: "failed" };
      }
      throw e;
    }

    // Step 4: notify — NotifyResult.version attests the variant
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const notifyResp = await (notificationClient as any).notify(
        { userId: req.userId, message: `Order ${orderId} confirmed`, orderId },
        restate.rpc.opts(applyXCanaryToRestateOptions({})),
      );
      auditTrail.push(`notification@${notifyResp.version}`);
    } catch (e) {
      if (e instanceof restate.TerminalError) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        await (paymentClient as any).refund(
          { orderId, amount: req.amount },
          restate.rpc.opts(applyXCanaryToRestateOptions({})),
        );
        return { ...order, status: "failed" };
      }
      throw e;
    }

    return { ...order, status: "completed" };
  });
}

export const checkoutSaga = restate.workflow({
  name: checkoutSagaDef.name,
  handlers: { run: checkoutSagaRunHandler },
});

export async function setupRestate(opts: RestateSetupOptions): Promise<void> {
  if (!opts.registerHandlers) {
    console.log("RESTATE_REGISTER_HANDLERS=false; skipping Restate endpoint listener");
    return;
  }
  console.log(`order-service Restate variant=${MY_VARIANT} binding ${checkoutSagaDef.name}`);
  await restate.endpoint().bind(checkoutSaga).listen(opts.port);
  console.log(`order-service Restate handlers listening on ${opts.port}`);
}
```

- [ ] **Step 2: Update http.ts to route by x-canary header**

Modify the `/api/orders` POST handler to construct the variant-suffixed URL:

```typescript
// services/order-service/src/http.ts (within app.post("/api/orders", ...))
const variant = req.headers["x-canary"] === "true" ? "Canary" : "Stable";
const result = await deps.ingressClient.post<Order>(
  `/CheckoutSaga${variant}/${orderId}/run`,
  body,
);
```

Also update the failed-order fallback to include `auditTrail: []`:

```typescript
const failed: Order = {
  id: orderId,
  userId: body.userId,
  sku: body.sku,
  quantity: body.quantity,
  amount: body.amount,
  status: "failed",
  auditTrail: [],
};
```

- [ ] **Step 3: Write saga-variant-binding test**

```typescript
// services/order-service/src/__tests__/saga-variant-binding.test.ts
import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";

describe("saga variant binding", () => {
  let originalVersion: string | undefined;
  beforeEach(() => {
    originalVersion = process.env.VERSION;
    vi.resetModules();
  });
  afterEach(() => {
    if (originalVersion === undefined) delete process.env.VERSION;
    else process.env.VERSION = originalVersion;
    vi.resetModules();
  });

  it("binds CheckoutSagaStable when VERSION is unset or 'stable'", async () => {
    delete process.env.VERSION;
    const mod = await import("../restate.js");
    expect(mod.checkoutSaga.name).toBe("CheckoutSagaStable");
  });

  it("binds CheckoutSagaCanary when VERSION is 'canary'", async () => {
    process.env.VERSION = "canary";
    const mod = await import("../restate.js");
    expect(mod.checkoutSaga.name).toBe("CheckoutSagaCanary");
  });
});
```

Run: `npm --workspace=@canary/order-service test -- saga-variant-binding`
Expected: 2/2 PASS.

- [ ] **Step 4: Update or add http test for routing**

Add cases asserting the variant-suffixed URL:

```typescript
// services/order-service/src/__tests__/http.test.ts (add cases)
it("posts to /CheckoutSagaCanary when x-canary: true", async () => {
  const ingressClient = { post: vi.fn().mockResolvedValue({ data: { /* ... */, auditTrail: ["saga@canary"] } }) };
  const app = setupHttp({ ingressClient: ingressClient as any });
  await request(app).post("/api/orders").set("x-canary", "true").send({ /* OrderRequest */ });
  expect(ingressClient.post).toHaveBeenCalledWith(
    expect.stringMatching(/^\/CheckoutSagaCanary\//),
    expect.any(Object),
  );
});

it("posts to /CheckoutSagaStable when x-canary absent", async () => {
  // mirror
});

it("posts to /CheckoutSagaStable when x-canary is 'false'", async () => {
  // assert non-"true" values do NOT trigger canary
});
```

Run: `npm --workspace=@canary/order-service test`
Expected: full suite PASS.

- [ ] **Step 5: Commit**

```bash
git add services/order-service/
git commit -m "feat(order-service): variant-aware CheckoutSaga + x-canary header routing"
```

---

### Task 7: notification-service — variant-aware notify

**Files:**
- Modify: `services/notification-service/src/restate.ts`
- Test: `services/notification-service/src/__tests__/restate.test.ts` (extend or create)

- [ ] **Step 1: Read existing notification restate.ts**

Read `services/notification-service/src/restate.ts` to capture the existing implementation (Phase 3.a TerminalError-on-`reject-me` driver).

- [ ] **Step 2: Refactor with variant binding + canary message suffix**

```typescript
// services/notification-service/src/restate.ts (full rewrite)
import * as restate from "@restatedev/restate-sdk";
import {
  notificationServiceStableDef,
  notificationServiceCanaryDef,
  type NotifyRequest,
  type NotifyResult,
} from "@canary/restate-defs-node";

export interface RestateSetupOptions {
  registerHandlers: boolean;
  port: number;
}

const MY_VARIANT: "stable" | "canary" =
  process.env.VERSION === "canary" ? "canary" : "stable";

const notificationServiceDef =
  MY_VARIANT === "canary" ? notificationServiceCanaryDef : notificationServiceStableDef;

export async function notifyHandler(
  ctx: restate.Context,
  req: NotifyRequest,
): Promise<NotifyResult> {
  // Phase 3.a TerminalError driver preserved.
  if (req.userId === "reject-me") {
    throw new restate.TerminalError(`notification rejected for userId=${req.userId}`);
  }

  const deliveredMessage =
    MY_VARIANT === "canary" ? `${req.message} [via canary notifier]` : req.message;

  // (preserve existing side effects: log, kafka emit, audit-call, store, etc.)

  return {
    delivered: true,
    version: MY_VARIANT,
    deliveredMessage,
  };
}

export const notificationService = restate.service({
  name: notificationServiceDef.name,
  handlers: { notify: notifyHandler },
});

export async function setupRestate(opts: RestateSetupOptions): Promise<void> {
  if (!opts.registerHandlers) {
    console.log("RESTATE_REGISTER_HANDLERS=false; skipping Restate endpoint listener");
    return;
  }
  console.log(`notification-service Restate variant=${MY_VARIANT} binding ${notificationServiceDef.name}`);
  await restate.endpoint().bind(notificationService).listen(opts.port);
  console.log(`notification-service Restate handlers listening on ${opts.port}`);
}
```

- [ ] **Step 3: Write or extend the unit test**

```typescript
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

describe("notification handler variant binding", () => {
  // (mirror saga-variant-binding pattern: VERSION env → notificationService.name)

  it("appends canary suffix when VERSION=canary", async () => {
    process.env.VERSION = "canary";
    vi.resetModules();
    const mod = await import("../restate.js");
    const result = await mod.notifyHandler({} as any, {
      userId: "u1", message: "Order x", orderId: "o1",
    });
    expect(result.deliveredMessage).toBe("Order x [via canary notifier]");
    expect(result.version).toBe("canary");
  });

  it("emits unmodified message when VERSION=stable", async () => {
    delete process.env.VERSION;
    vi.resetModules();
    const mod = await import("../restate.js");
    const result = await mod.notifyHandler({} as any, {
      userId: "u1", message: "Order x", orderId: "o1",
    });
    expect(result.deliveredMessage).toBe("Order x");
    expect(result.version).toBe("stable");
  });

  it("preserves Phase 3.a TerminalError on reject-me", async () => {
    delete process.env.VERSION;
    vi.resetModules();
    const restate = await import("@restatedev/restate-sdk");
    const mod = await import("../restate.js");
    await expect(mod.notifyHandler({} as any, {
      userId: "reject-me", message: "x", orderId: "o1",
    })).rejects.toBeInstanceOf(restate.TerminalError);
  });
});
```

Run: `npm --workspace=@canary/notification-service test`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add services/notification-service/
git commit -m "feat(notification-service): variant-aware notify with NotifyResult"
```

---

### Task 8: Helm chart — per-subset Services + variant-aware registration

**Files:**
- Modify: `deploy/helm/service-chart/templates/service.yaml`
- Modify: `deploy/helm/service-chart/templates/restate-register-job.yaml`
- Modify: `deploy/helm/service-chart/templates/deployment.yaml`
- Inspect/modify: `deploy/helm/values/canary-overlay.yaml` (if it sets `RESTATE_REGISTER_HANDLERS=false`)

- [ ] **Step 1: Update templates/service.yaml to render shared + per-subset Services**

```yaml
# deploy/helm/service-chart/templates/service.yaml
{{- $version := .Values.version | default "stable" -}}
{{- if eq $version "stable" }}
# Shared Service — used by Istio VirtualService/DestinationRule for HTTP routing.
# Only rendered on the stable release; canary release skips this block.
apiVersion: v1
kind: Service
metadata:
  name: {{ .Values.serviceName }}
  namespace: {{ .Release.Namespace }}
  labels:
    app: {{ .Values.serviceName }}
spec:
  type: ClusterIP
  selector:
    app: {{ .Values.serviceName }}
  ports:
    - name: http
      port: {{ .Values.ports.http }}
      targetPort: http
    - name: restate
      port: {{ .Values.ports.restateHandler }}
      targetPort: restate
{{- end }}
---
# Per-subset Service — used by Restate to dispatch to one variant only.
# Rendered on every release (stable AND canary), one per variant.
apiVersion: v1
kind: Service
metadata:
  name: {{ .Values.serviceName }}-{{ $version }}
  namespace: {{ .Release.Namespace }}
  labels:
    app: {{ .Values.serviceName }}
    version: {{ $version }}
spec:
  type: ClusterIP
  selector:
    app: {{ .Values.serviceName }}
    version: {{ $version }}
  ports:
    - name: http
      port: {{ .Values.ports.http }}
      targetPort: http
    - name: restate
      port: {{ .Values.ports.restateHandler }}
      targetPort: restate
```

- [ ] **Step 2: Update restate-register-job.yaml to use variant-suffixed URL**

Change line 31 of `restate-register-job.yaml`:

```yaml
# before:
              SVC="{{ .Values.serviceName }}.{{ .Release.Namespace }}.svc.cluster.local"
# after:
              SVC="{{ .Values.serviceName }}-{{ .Values.version | default "stable" }}.{{ .Release.Namespace }}.svc.cluster.local"
```

This makes the registered deployment URL variant-specific. Stable release registers `<name>-stable...`; canary registers `<name>-canary...`.

- [ ] **Step 3: Lift Restate gates on canary**

Two settings in `deploy/helm/values/canary-overlay.yaml` gate Restate registration on canary today (verified 2026-05-11):

```yaml
# canary-overlay.yaml (current state)
env:
  RESTATE_REGISTER_HANDLERS: "false"     # line 18 — gates the Restate endpoint server inside the pod
restate:
  registerEndpoint: false                 # line 28 — gates the post-install Helm Job (restate-register-job.yaml)
```

Both must be lifted for Phase 3.b. Update `canary-overlay.yaml`:

```yaml
# canary-overlay.yaml (Phase 3.b)
version: canary
replicas: 1
env:
  KAFKA_CONSUMERS_ENABLED: "true"
  # RESTATE_REGISTER_HANDLERS removed — canary now registers handlers (Phase 3.b).
  # Default from values.yaml ("true") applies.
  MANAGEMENT_ENDPOINT_HEALTH_GROUP_READINESS_INCLUDE: "readinessState,kafkaConsumer"
restate:
  registerEndpoint: true   # canary now runs the post-install registration Job
```

Also confirm: `deploy/helm/service-chart/values.yaml` keeps `RESTATE_REGISTER_HANDLERS: "true"` as the default, and `restate.registerEndpoint` should default to `true` (verify and add if missing).

Comment in `canary-overlay.yaml` header should be updated to reflect that "canary pods do not register Restate handlers" is no longer accurate post-3.b.

Variant identification: confirm canary's `version: canary` label and the `app.version=canary` Spring property both flow from the same source (the `version` Helm value). The variant-conditional Spring beans in Tasks 4-5 read `app.version`, so the Helm chart must inject `-Dapp.version={{ .Values.version }}` (or equivalent) into the JVM args. If this isn't already done, add it to `deployment.yaml`. (Read the existing deployment.yaml to confirm.)

- [ ] **Step 4: Helm-lint the chart**

Run: `helm lint deploy/helm/service-chart`
Expected: no errors.

- [ ] **Step 5: Helm template render with stable values (sanity check)**

Run: `helm template inventory-stable deploy/helm/service-chart -f deploy/helm/values/inventory-service.yaml --set version=stable`
Expected output contains: `inventory-service` Service AND `inventory-service-stable` Service AND a register job posting URL `inventory-service-stable.services.svc.cluster.local`.

Run: `helm template inventory-canary deploy/helm/service-chart -f deploy/helm/values/inventory-service.yaml --set version=canary`
Expected: only `inventory-service-canary` Service (no `inventory-service` shared Service); register job URL = `inventory-service-canary.services.svc.cluster.local`.

- [ ] **Step 6: Commit**

```bash
git add deploy/helm/
git commit -m "chore(helm): per-subset K8s Services + variant-aware Restate registration"
```

---

### Task 9: e2e — extend R1-R5 saga tests for variant assertions

**Files:**
- Modify: `tests/e2e/r1-r5-restate-saga.test.ts`

- [ ] **Step 1: Inspect the current R1-R5 file structure**

Read the file. Identify the existing scenario blocks (R1 happy, R2 charge fails, R3 notify fails, R4 timer expiry, R5 confirm-after-release).

- [ ] **Step 2: Parameterize scenarios across variants**

For each scenario, run it twice — once with `x-canary: true` header, once without. Assert variant-specific fields:

```typescript
describe.each([
  { variant: "stable" as const, headers: {} },
  { variant: "canary" as const, headers: { "x-canary": "true" } },
])("R1 happy path — $variant", ({ variant, headers }) => {
  it("completes with matching variant in auditTrail and tweaks", async () => {
    const orderRequest = { userId: "u1", sku: "SKU1", quantity: 2, amount: 1000 };
    const resp = await axios.post(`${ORDER_SERVICE_URL}/api/orders`, orderRequest, { headers });
    expect(resp.status).toBe(201);
    const order = resp.data;
    expect(order.status).toBe("completed");
    expect(order.auditTrail).toEqual([
      `saga@${variant}`,
      `reservation@${variant}`,
      `payment@${variant}`,
      `notification@${variant}`,
    ]);
    // The actual charged amount can be observed via /api/orders/<id> if the saga
    // exposes it, OR via the payment-service charge store; assert via whichever
    // the existing R1 already inspects. If R1 doesn't inspect this, skip.
  });
});
```

Apply the same pattern to R2-R5. For R4-R5 (slow), keep the `RUN_SLOW_E2E` gate.

- [ ] **Step 3: Run extended R1-R3 (fast)**

Run: `npm --workspace=@canary/tests-e2e test -- r1-r5`
Expected: R1-R3 PASS for both variants. R4-R5 SKIP (gated).

- [ ] **Step 4: Commit**

```bash
git add tests/e2e/
git commit -m "test(e2e): parameterize R1-R5 across stable/canary variants"
```

---

### Task 10: e2e — R6 subset isolation (fast)

**Files:**
- Create: `tests/e2e/r6-restate-canary-isolation.test.ts`

- [ ] **Step 1: Write the test**

```typescript
import { describe, it, expect } from "vitest";
import axios from "axios";

const ORDER_SERVICE_URL = process.env.ORDER_SERVICE_URL ?? "http://localhost:8080";

describe("R6 — Restate subset isolation under concurrent traffic", () => {
  it("flagged and unflagged concurrent orders each traverse their own subset end-to-end", async () => {
    const N = 10;
    const orderRequest = (i: number) => ({
      userId: `u${i}`, sku: "SKU1", quantity: 1, amount: 1000,
    });

    const promises = Array.from({ length: N }, (_, i) => {
      const flagged = i % 2 === 0;
      return axios
        .post(`${ORDER_SERVICE_URL}/api/orders`, orderRequest(i), {
          headers: flagged ? { "x-canary": "true" } : {},
        })
        .then((resp) => ({ flagged, order: resp.data }));
    });

    const results = await Promise.all(promises);

    for (const { flagged, order } of results) {
      const variant = flagged ? "canary" : "stable";
      expect(order.status).toBe("completed");
      expect(order.auditTrail).toEqual([
        `saga@${variant}`,
        `reservation@${variant}`,
        `payment@${variant}`,
        `notification@${variant}`,
      ]);
    }

    // Cross-contamination assertion: no flagged order has any "@stable" entry,
    // and vice versa.
    for (const { flagged, order } of results) {
      const wrong = flagged ? "@stable" : "@canary";
      for (const entry of order.auditTrail) {
        expect(entry).not.toContain(wrong);
      }
    }
  }, 60_000);
});
```

- [ ] **Step 2: Run R6**

Run: `npm --workspace=@canary/tests-e2e test -- r6`
Expected: PASS within 60s.

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/r6-restate-canary-isolation.test.ts
git commit -m "test(e2e): R6 subset isolation under concurrent stable+canary traffic"
```

---

### Task 11: e2e — R7 canary deploy lifecycle (cluster-verify gated)

**Files:**
- Create: `tests/e2e/r7-restate-canary-deploy-lifecycle.test.ts`

- [ ] **Step 1: Write the gated lifecycle test**

```typescript
import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import axios from "axios";

const execFileAsync = promisify(execFile);
const RUN = process.env.RUN_CANARY_LIFECYCLE_TESTS === "true";
const RESTATE_ADMIN_URL = process.env.RESTATE_ADMIN_URL ?? "http://localhost:9070";

(RUN ? describe : describe.skip)("R7 — Restate canary deployment lifecycle", () => {
  it("both variant deployments register without conflict", async () => {
    const resp = await axios.get(`${RESTATE_ADMIN_URL}/services`);
    const names = resp.data.services.map((s: { name: string }) => s.name);
    for (const base of ["CheckoutSaga", "ReservationWorkflow", "PaymentVO", "NotificationService"]) {
      expect(names).toContain(`${base}Stable`);
      expect(names).toContain(`${base}Canary`);
    }
  });

  it("per-subset K8s Services have correct selectors", async () => {
    for (const svc of ["inventory-service", "payment-service", "order-service", "notification-service"]) {
      for (const variant of ["stable", "canary"]) {
        const { stdout } = await execFileAsync(
          "kubectl",
          ["-n", "services", "get", "svc", `${svc}-${variant}`,
            "-o", "jsonpath={.spec.selector.version}"],
          { encoding: "utf8" },
        );
        expect(stdout.trim()).toBe(variant);
      }
    }
  });

  it("concurrent flagged + unflagged orders maintain isolation (cluster path)", async () => {
    // Same shape as R6 but pointed at the in-cluster order-service URL.
    // ... (mirror R6 body)
  });

  it("retiring canary leaves stable functional", async () => {
    // helm uninstall canary release
    await execFileAsync("helm", ["uninstall", "inventory-canary", "-n", "services"]);
    // optionally also for the other services if R7 is comprehensive

    // Wait for canary pod removal
    await execFileAsync("kubectl", ["wait", "pod", "-l", "app=inventory-service,version=canary",
        "-n", "services", "--for=delete", "--timeout=60s"]);

    // Issue a flagged request — expect Restate Ingress to surface failure
    // (404 or similar) since CheckoutSagaCanary deployment URL is unreachable
    // (or has been DELETE'd from Restate Admin if the test does so explicitly).
    // Assert stable still works.

    const stableResp = await axios.post(
      `${process.env.ORDER_SERVICE_URL}/api/orders`,
      { userId: "u1", sku: "SKU1", quantity: 1, amount: 1000 },
      // no x-canary header
    );
    expect(stableResp.status).toBe(201);
    expect(stableResp.data.auditTrail).toEqual([
      "saga@stable", "reservation@stable", "payment@stable", "notification@stable",
    ]);

    // Note: this test does not re-install canary. Operator must redeploy after.
  }, 180_000);
});
```

- [ ] **Step 2: Verify R7 is correctly gated (skipped without env var)**

Run: `npm --workspace=@canary/tests-e2e test -- r7`
Expected: SKIP (no RUN_CANARY_LIFECYCLE_TESTS=true).

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/r7-restate-canary-deploy-lifecycle.test.ts
git commit -m "test(e2e): R7 canary deploy lifecycle (cluster-verify gated)"
```

---

## Final integration check

After all tasks committed:

- [ ] **Step 1: Full unit + integration test sweep**

Run: `./gradlew test` (Java) and `npm test` (Node workspaces)
Expected: all pass.

- [ ] **Step 2: Verify Helm chart renders cleanly for both variants of all services**

Run: `helm template <name>-stable deploy/helm/service-chart -f deploy/helm/values/<name>.yaml --set version=stable` for each of inventory, payment, order, notification, audit. Then again with `--set version=canary`. Check that:
- Stable releases produce both `<name>` and `<name>-stable` Services.
- Canary releases produce only `<name>-canary` Service.
- Registration job URLs match the variant.

- [ ] **Step 3: Cluster verify (optional — requires kind cluster)**

Run: `make undeploy-services && make deploy-services && make canary-deploy` against a kind cluster, then `RUN_CANARY_LIFECYCLE_TESTS=true npm --workspace=@canary/tests-e2e test -- r7`.

This proves F1/F2/F3 operationally. Defer to user if cluster verify is out-of-scope for the implementation phase.

- [ ] **Step 4: Update operations/docs files if any reference the old single deployment_id model or `RESTATE_REGISTER_HANDLERS=false` constraint**

Grep for `RESTATE_REGISTER_HANDLERS` in docs. Update wording to reflect that both variants register; the constraint has been lifted.

```bash
grep -rn "RESTATE_REGISTER_HANDLERS" docs/ services/*/README.md 2>/dev/null
```

Update any stale references.

- [ ] **Step 5: Final commit (docs + any small cleanups)**

```bash
git add -p   # selective
git commit -m "docs: update Restate registration narrative for Phase 3.b"
```

---

## Out-of-scope (deferred)

- **Restate-native α path** (rolling versioning with in-flight drainage). Documented in spec § Alternatives Considered.
- **Automatic canary teardown tooling** that drains in-flights before unregistering. Operator runbook documents the manual sequence (spec § Operational runbook).
- **Cross-cluster Restate / multi-region**. Out of scope.
- **Behavioral tweaks beyond the four documented**. We picked one observable tweak per handler; adding more is a follow-up phase.
