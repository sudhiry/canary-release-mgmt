# Phase 1.3.a — Domain Services Code Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement 5 domain services (3 Java + Spring Boot, 2 TS + Node) per the [Plan 1.3.a design](../specs/2026-05-08-canary-release-phase-1-3-a-services-design.md) — thin in-memory state, Restate handlers gated by feature flags, pure unit tests with all I/O mocked. No deployment artifacts (those are 1.3.b).

**Architecture:** Each service is a self-contained module (Java services as Gradle subprojects under `services/`, Node services as pnpm workspace packages). HTTP controllers delegate to their own Restate handler via Ingress (option β); handlers do work + Restate-to-Restate audit fan-out. Order-svc is the exception: HTTP fan-out lives in the controller; CheckoutSaga is a stub. Two new shared modules (`platform/restate-defs-java`, `platform/restate-defs-node`) carry the cross-service Restate type contracts.

**Tech Stack:**
- **Java side:** JDK 25 + Spring Boot 4.0.4 + Gradle Kotlin DSL + JUnit 6 + Mockito + AssertJ + Spring Kafka + Restate Java SDK 2.7.0 (`sdk-api`, `sdk-common`, `sdk-http-vertx`).
- **Node side:** Node 22 LTS + TypeScript 5.x (strict, ESM) + pnpm workspaces + Express + axios + KafkaJS + `@restatedev/restate-sdk` 1.14.2 + Vitest + supertest.

**Spec reference:** `docs/superpowers/specs/2026-05-08-canary-release-phase-1-3-a-services-design.md`

---

## Prerequisites (user installs — do NOT auto-install)

Same as Plan 1.2. Verify:

```
java --version          # JDK 25.x.x
./gradlew --version     # Gradle 9.5.0
node --version          # v22.x or newer
pnpm --version          # 9.x or newer
```

If any are missing, **stop and report NEEDS_CONTEXT**.

---

## File Structure (additions in 1.3.a)

```
canary-release-mgmt/
├── settings.gradle.kts                                # +4 includes (restate-defs-java + 3 Java services)
├── pnpm-workspace.yaml                                # +3 globs (restate-defs-node + 2 Node services)
├── gradle/libs.versions.toml                          # +1 lib (restate-sdk-http-vertx)
├── Makefile                                           # update verify; add build-services
├── README.md                                          # update for 1.3.a status
├── platform/
│   ├── restate-defs-java/                             # NEW
│   │   ├── build.gradle.kts
│   │   └── src/main/java/com/canary/restate/
│   │       ├── audit/{AuditEvent.java,AuditQueryService.java}
│   │       ├── payment/{ChargeRequest.java,Charge.java,PaymentVO.java}
│   │       ├── inventory/{ReservationRequest.java,Reservation.java,AvailabilityResponse.java,ReservationWorkflow.java}
│   │       ├── notification/{NotifyRequest.java,Notification.java,NotificationService.java}
│   │       └── order/{OrderRequest.java,Order.java,CheckoutSaga.java}
│   └── restate-defs-node/                             # NEW
│       ├── package.json
│       ├── tsconfig.json
│       └── src/index.ts                               # all DTOs + service defs
└── services/                                          # NEW directory
    ├── audit-service/                                 # Java + Spring Boot
    │   ├── build.gradle.kts
    │   └── src/{main,test}/java/com/canary/audit/...
    ├── payment-service/                               # Java + Spring Boot
    │   ├── build.gradle.kts
    │   └── src/{main,test}/java/com/canary/payment/...
    ├── inventory-service/                             # Java + Spring Boot
    │   ├── build.gradle.kts
    │   └── src/{main,test}/java/com/canary/inventory/...
    ├── notification-service/                          # TS + Node
    │   ├── package.json, tsconfig.json, vitest.config.ts
    │   └── src/{index.ts, http.ts, kafka.ts, restate.ts, store.ts, __tests__/...}
    └── order-service/                                 # TS + Node
        ├── package.json, tsconfig.json, vitest.config.ts
        └── src/{index.ts, http.ts, saga.ts, kafka.ts, restate.ts, store.ts, __tests__/...}
```

---

## Notes for implementers — Restate SDK guidance

The Restate Java SDK 2.7.0 and Node SDK 1.14.2 are wire-compatible with the running Restate server (1.6.2). Specific API patterns assumed in this plan:

- **Java annotations** (in `dev.restate.sdk.annotation.*`): `@Service`, `@VirtualObject`, `@Workflow`, `@Handler`. Context types in `dev.restate.sdk.*`: `Context` (plain service), `ObjectContext` (virtual object — provides `key()`), `WorkflowContext` (workflow — provides `key()`/workflow id).
- **Java cross-service call from inside a handler:** `ctx.serviceClient(MyService.class).call(MyService::myHandler, input, opts)` — the `opts` is built via `XCanaryRestateClientCustomizer.apply(InvocationOptions.builder())`. Annotated abstract classes in `restate-defs-java` produce SDK-generated client classes via the SDK's annotation processor.
- **Java HTTP endpoint for serving handlers:** `RestateHttpEndpointBuilder.builder().bind(handlerImpl).listen(port)` from `dev.restate:sdk-http-vertx`. Wrap in a `@Bean` gated by `@ConditionalOnProperty(name = "app.restate.register-handlers", havingValue = "true", matchIfMissing = true)`.
- **Node:** `restate.service({name, handlers})`, `restate.object({name, handlers})`, `restate.workflow({name, handlers})`, `restate.endpoint().bind(svc).http2Listener(port)`, `ctx.serviceClient(def).method(input, applyXCanaryToRestateOptions({}))`.
- **Restate Ingress URL paths** (server 1.6.2): `POST {ingressUrl}/{ServiceName}/{handler}` for plain Service; `POST {ingressUrl}/{ServiceName}/{key}/{handler}` for VirtualObject and Workflow.
- **If exact API surface differs in your SDK build:** preserve task structure; adjust import paths and method names to match the actual SDK. Don't bypass the abstraction (e.g., don't drop x-canary stamping just because the SDK API is awkward).

The HTTP-controller→Ingress hop uses Spring's `RestClient` (Java) or axios (Node), pointed at `RESTATE_INGRESS_URL`. The 1.2 lib HTTP interceptors stamp `x-canary` automatically — no Restate-specific wrapper needed for the controller→Ingress hop.

---

## Phase A — Shared platform additions

### Task 1: Create `platform/restate-defs-java` module

**Files:**
- Create: `platform/restate-defs-java/build.gradle.kts`
- Create: `platform/restate-defs-java/src/main/java/com/canary/restate/audit/AuditEvent.java`
- Create: `platform/restate-defs-java/src/main/java/com/canary/restate/audit/AuditQueryService.java`
- Create: `platform/restate-defs-java/src/main/java/com/canary/restate/payment/ChargeRequest.java`
- Create: `platform/restate-defs-java/src/main/java/com/canary/restate/payment/Charge.java`
- Create: `platform/restate-defs-java/src/main/java/com/canary/restate/payment/PaymentVO.java`
- Create: `platform/restate-defs-java/src/main/java/com/canary/restate/inventory/ReservationRequest.java`
- Create: `platform/restate-defs-java/src/main/java/com/canary/restate/inventory/Reservation.java`
- Create: `platform/restate-defs-java/src/main/java/com/canary/restate/inventory/AvailabilityResponse.java`
- Create: `platform/restate-defs-java/src/main/java/com/canary/restate/inventory/ReservationWorkflow.java`
- Create: `platform/restate-defs-java/src/main/java/com/canary/restate/notification/NotifyRequest.java`
- Create: `platform/restate-defs-java/src/main/java/com/canary/restate/notification/Notification.java`
- Create: `platform/restate-defs-java/src/main/java/com/canary/restate/notification/NotificationService.java`
- Create: `platform/restate-defs-java/src/main/java/com/canary/restate/order/OrderRequest.java`
- Create: `platform/restate-defs-java/src/main/java/com/canary/restate/order/Order.java`
- Create: `platform/restate-defs-java/src/main/java/com/canary/restate/order/CheckoutSaga.java`

- [ ] **Step 1: Write `platform/restate-defs-java/build.gradle.kts`**

```kotlin
plugins {
    `java-library`
}

dependencies {
    api(libs.restate.sdk.api)
    api(libs.restate.sdk.common)
}
```

- [ ] **Step 2: Write all DTO records**

`platform/restate-defs-java/src/main/java/com/canary/restate/audit/AuditEvent.java`:

```java
package com.canary.restate.audit;

public record AuditEvent(String aggregate, String id, String action, String correlationId) {}
```

`platform/restate-defs-java/src/main/java/com/canary/restate/payment/ChargeRequest.java`:

```java
package com.canary.restate.payment;

public record ChargeRequest(String orderId, long amount) {}
```

`platform/restate-defs-java/src/main/java/com/canary/restate/payment/Charge.java`:

```java
package com.canary.restate.payment;

public record Charge(String id, String orderId, long amount, String status) {}
```

`platform/restate-defs-java/src/main/java/com/canary/restate/inventory/ReservationRequest.java`:

```java
package com.canary.restate.inventory;

public record ReservationRequest(String sku, int quantity, String orderId) {}
```

`platform/restate-defs-java/src/main/java/com/canary/restate/inventory/Reservation.java`:

```java
package com.canary.restate.inventory;

public record Reservation(String id, String sku, int quantity, String orderId, String status) {}
```

`platform/restate-defs-java/src/main/java/com/canary/restate/inventory/AvailabilityResponse.java`:

```java
package com.canary.restate.inventory;

public record AvailabilityResponse(String sku, int available) {}
```

`platform/restate-defs-java/src/main/java/com/canary/restate/notification/NotifyRequest.java`:

```java
package com.canary.restate.notification;

public record NotifyRequest(String userId, String message, String orderId) {}
```

`platform/restate-defs-java/src/main/java/com/canary/restate/notification/Notification.java`:

```java
package com.canary.restate.notification;

public record Notification(String id, String userId, String message, String status) {}
```

`platform/restate-defs-java/src/main/java/com/canary/restate/order/OrderRequest.java`:

```java
package com.canary.restate.order;

public record OrderRequest(String userId, String sku, int quantity, long amount) {}
```

`platform/restate-defs-java/src/main/java/com/canary/restate/order/Order.java`:

```java
package com.canary.restate.order;

public record Order(String id, String userId, String sku, int quantity, long amount, String status) {}
```

- [ ] **Step 3: Write all Restate service definitions (abstract classes)**

`platform/restate-defs-java/src/main/java/com/canary/restate/audit/AuditQueryService.java`:

```java
package com.canary.restate.audit;

import dev.restate.sdk.Context;
import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.Service;

import java.util.List;

@Service
public abstract class AuditQueryService {
    @Handler
    public abstract void append(Context ctx, AuditEvent event);

    @Handler
    public abstract List<AuditEvent> byAggregate(Context ctx, String aggregateId);
}
```

`platform/restate-defs-java/src/main/java/com/canary/restate/payment/PaymentVO.java`:

```java
package com.canary.restate.payment;

import dev.restate.sdk.ObjectContext;
import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.VirtualObject;

@VirtualObject
public abstract class PaymentVO {
    @Handler
    public abstract Charge charge(ObjectContext ctx, ChargeRequest req);
}
```

`platform/restate-defs-java/src/main/java/com/canary/restate/inventory/ReservationWorkflow.java`:

```java
package com.canary.restate.inventory;

import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.Workflow;

@Workflow
public abstract class ReservationWorkflow {
    @Handler
    public abstract Reservation run(WorkflowContext ctx, ReservationRequest req);
}
```

`platform/restate-defs-java/src/main/java/com/canary/restate/notification/NotificationService.java`:

```java
package com.canary.restate.notification;

import dev.restate.sdk.Context;
import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.Service;

@Service
public abstract class NotificationService {
    @Handler
    public abstract Notification notify(Context ctx, NotifyRequest req);
}
```

`platform/restate-defs-java/src/main/java/com/canary/restate/order/CheckoutSaga.java`:

```java
package com.canary.restate.order;

import dev.restate.sdk.WorkflowContext;
import dev.restate.sdk.annotation.Handler;
import dev.restate.sdk.annotation.Workflow;

@Workflow
public abstract class CheckoutSaga {
    @Handler
    public abstract Order run(WorkflowContext ctx, OrderRequest req);
}
```

- [ ] **Step 4: Run compilation to verify**

Run: `./gradlew :platform:restate-defs-java:compileJava --quiet`

Expected: BUILD SUCCESSFUL. If `:platform:restate-defs-java` is not registered yet (Task 3 hasn't run), this fails — that's fine; verify the compilation in Task 3.

- [ ] **Step 5: Commit**

```bash
git add platform/restate-defs-java/
git commit -m "feat(platform): add restate-defs-java with cross-service Restate types"
```

---

### Task 2: Create `platform/restate-defs-node` package

**Files:**
- Create: `platform/restate-defs-node/package.json`
- Create: `platform/restate-defs-node/tsconfig.json`
- Create: `platform/restate-defs-node/src/index.ts`

- [ ] **Step 1: Write `platform/restate-defs-node/package.json`**

```json
{
  "name": "@canary/restate-defs-node",
  "version": "0.0.1",
  "private": true,
  "type": "module",
  "main": "./dist/index.js",
  "types": "./dist/index.d.ts",
  "exports": {
    ".": {
      "import": "./dist/index.js",
      "types": "./dist/index.d.ts"
    }
  },
  "scripts": {
    "build": "tsc -p tsconfig.json"
  },
  "dependencies": {
    "@restatedev/restate-sdk": "^1.14.2"
  },
  "devDependencies": {
    "typescript": "^5.6.2"
  }
}
```

- [ ] **Step 2: Write `platform/restate-defs-node/tsconfig.json`**

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ESNext",
    "moduleResolution": "Bundler",
    "lib": ["ES2022"],
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "outDir": "./dist",
    "rootDir": "./src",
    "declaration": true,
    "declarationMap": true,
    "resolveJsonModule": true,
    "isolatedModules": true
  },
  "include": ["src/**/*.ts"],
  "exclude": ["dist", "node_modules"]
}
```

- [ ] **Step 3: Write `platform/restate-defs-node/src/index.ts`**

```typescript
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
  amount: number;
  status: string;
}

export type PaymentVOMethods = {
  charge(req: ChargeRequest): Promise<Charge>;
};

export const paymentVODef = {
  name: "PaymentVO",
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
}

export interface AvailabilityResponse {
  sku: string;
  available: number;
}

export type ReservationWorkflowMethods = {
  run(req: ReservationRequest): Promise<Reservation>;
};

export const reservationWorkflowDef = {
  name: "ReservationWorkflow",
} as restate.WorkflowDefinitionFrom<ReservationWorkflowMethods>;

// ----- notification -----
export interface NotifyRequest {
  userId: string;
  message: string;
  orderId: string;
}

export interface Notification {
  id: string;
  userId: string;
  message: string;
  status: string;
}

export type NotificationServiceMethods = {
  notify(req: NotifyRequest): Promise<Notification>;
};

export const notificationServiceDef = {
  name: "NotificationService",
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
}

export type CheckoutSagaMethods = {
  run(req: OrderRequest): Promise<Order>;
};

export const checkoutSagaDef = {
  name: "CheckoutSaga",
} as restate.WorkflowDefinitionFrom<CheckoutSagaMethods>;
```

If `restate.ServiceDefinitionFrom` / `VirtualObjectDefinitionFrom` / `WorkflowDefinitionFrom` aren't the exact type names in `@restatedev/restate-sdk@1.14.2` (they may be `ServiceDefinition<…>` etc.), adjust to the actual exported types — preserve the shape `{ name: string }` as the runtime value plus the typed compile-time facade.

- [ ] **Step 4: Verify build**

After Task 3 wires this into the workspace:

Run: `pnpm --filter @canary/restate-defs-node build`
Expected: produces `dist/index.js` and `dist/index.d.ts`. No errors.

- [ ] **Step 5: Commit**

```bash
git add platform/restate-defs-node/
git commit -m "feat(platform): add restate-defs-node with cross-service Restate types + DTOs"
```

---

### Task 3: Wire shared modules into root build + workspace; add `services/` directory

**Files:**
- Modify: `settings.gradle.kts`
- Modify: `pnpm-workspace.yaml`
- Modify: `gradle/libs.versions.toml`
- Create: `services/.gitkeep`

- [ ] **Step 1: Update `settings.gradle.kts`**

Replace the existing content:

```kotlin
rootProject.name = "canary-release-mgmt"

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

include("platform:lib-java")
include("platform:restate-defs-java")
```

(Java service includes are added in their respective scaffold tasks — Tasks 4, 8, 12.)

- [ ] **Step 2: Update `pnpm-workspace.yaml`**

Replace the existing content:

```yaml
packages:
  - "platform/lib-node"
  - "platform/restate-defs-node"
  - "services/*"
```

The `services/*` glob will pick up Node services as they're added (Tasks 16, 20). Java services have no `package.json` so pnpm skips them.

- [ ] **Step 3: Update `gradle/libs.versions.toml`**

Add a line under `[libraries]` (alphabetical order beside the existing `restate-sdk-*` entries):

```toml
restate-sdk-http-vertx         = { module = "dev.restate:sdk-http-vertx",     version.ref = "restateSdk" }
```

- [ ] **Step 4: Create `services/.gitkeep`**

```bash
mkdir -p services && touch services/.gitkeep
```

The Java service Gradle subprojects (Tasks 4, 8, 12) and Node service pnpm packages (Tasks 16, 20) populate this directory. The `.gitkeep` ensures the directory is tracked even before any service exists.

- [ ] **Step 5: Run pnpm install + Gradle compile to verify**

Run: `pnpm install`
Expected: pnpm picks up the new workspace, links `@canary/restate-defs-node`. No errors.

Run: `./gradlew :platform:restate-defs-java:compileJava --quiet`
Expected: BUILD SUCCESSFUL. The DTO records and abstract Restate definitions compile cleanly.

Run: `pnpm --filter @canary/restate-defs-node build`
Expected: produces `dist/index.js` and `dist/index.d.ts`. No errors.

- [ ] **Step 6: Commit**

```bash
git add settings.gradle.kts pnpm-workspace.yaml gradle/libs.versions.toml services/.gitkeep
git commit -m "feat(build): register restate-defs-{java,node} and reserve services/ directory"
```

---

## Phase B — audit-service (Java, terminal)

audit-service is the first service to implement because every other service ends up calling it (HTTP and Restate-to-Restate). It has no outbound calls to other services.

### Task 4: Scaffold `services/audit-service` Gradle subproject

**Files:**
- Modify: `settings.gradle.kts`
- Create: `services/audit-service/build.gradle.kts`
- Create: `services/audit-service/src/main/resources/application.yml`
- Create: `services/audit-service/src/test/resources/application-test.yml`
- Create: `services/audit-service/src/main/java/com/canary/audit/AuditApplication.java`
- Create: `services/audit-service/src/test/java/com/canary/audit/AuditApplicationTest.java`

- [ ] **Step 1: Add `services:audit-service` to `settings.gradle.kts`**

Add the line below the `include("platform:restate-defs-java")` line:

```kotlin
include("services:audit-service")
```

- [ ] **Step 2: Write `services/audit-service/build.gradle.kts`**

```kotlin
plugins {
    java
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
}

dependencies {
    implementation(project(":platform:lib-java"))
    implementation(project(":platform:restate-defs-java"))
    implementation(libs.spring.boot.starter.web)
    implementation(libs.spring.kafka)
    implementation(libs.restate.sdk.api)
    implementation(libs.restate.sdk.common)
    implementation(libs.restate.sdk.http.vertx)

    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.mockito.core)
    testImplementation(libs.assertj.core)
    testRuntimeOnly(libs.junit.platform.launcher)
}
```

- [ ] **Step 3: Write `services/audit-service/src/main/resources/application.yml`**

```yaml
server:
  port: 8083

spring:
  application:
    name: audit-service
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}

app:
  kafka:
    consumers:
      enabled: ${KAFKA_CONSUMERS_ENABLED:true}
  restate:
    register-handlers: ${RESTATE_REGISTER_HANDLERS:true}
    ingress:
      url: ${RESTATE_INGRESS_URL:http://localhost:9070}
    handler:
      port: ${RESTATE_HANDLER_PORT:9083}
```

- [ ] **Step 4: Write `services/audit-service/src/test/resources/application-test.yml`**

```yaml
server:
  port: 0

spring:
  kafka:
    bootstrap-servers: localhost:0

app:
  kafka:
    consumers:
      enabled: false
  restate:
    register-handlers: false
    ingress:
      url: http://example.invalid
    handler:
      port: 0
```

The test profile keeps both flags off so the `@SpringBootTest` smoke test doesn't try to bind real ports or connect to a real broker.

- [ ] **Step 5: Write `services/audit-service/src/main/java/com/canary/audit/AuditApplication.java`**

```java
package com.canary.audit;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class AuditApplication {
    public static void main(String[] args) {
        SpringApplication.run(AuditApplication.class, args);
    }
}
```

- [ ] **Step 6: Write the failing smoke test**

`services/audit-service/src/test/java/com/canary/audit/AuditApplicationTest.java`:

```java
package com.canary.audit;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("test")
class AuditApplicationTest {

    @Test
    void contextLoads() {
        // Spring context starts cleanly with both gating flags off.
    }
}
```

- [ ] **Step 7: Run the test to verify it passes**

Run: `./gradlew :services:audit-service:test --quiet`

Expected: BUILD SUCCESSFUL, 1 test passes.

- [ ] **Step 8: Commit**

```bash
git add settings.gradle.kts services/audit-service/
git commit -m "feat(audit-service): scaffold Gradle subproject + smoke test"
```

---

### Task 5: AuditController (POST /audit/events delegates via Ingress; GET /audit/by-aggregate/{id} reads store)

**Files:**
- Create: `services/audit-service/src/main/java/com/canary/audit/store/AuditEventStore.java`
- Create: `services/audit-service/src/main/java/com/canary/audit/config/IngressClientConfig.java`
- Create: `services/audit-service/src/main/java/com/canary/audit/controller/AuditController.java`
- Create: `services/audit-service/src/test/java/com/canary/audit/controller/AuditControllerTest.java`

- [ ] **Step 1: Write `AuditEventStore`**

```java
package com.canary.audit.store;

import com.canary.restate.audit.AuditEvent;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

@Component
public class AuditEventStore {

    private final List<AuditEvent> events = new CopyOnWriteArrayList<>();

    public void append(AuditEvent event) {
        events.add(event);
    }

    public List<AuditEvent> findByAggregate(String aggregate) {
        return events.stream()
            .filter(e -> aggregate.equals(e.aggregate()))
            .collect(Collectors.toList());
    }

    public List<AuditEvent> all() {
        return new ArrayList<>(events);
    }
}
```

`CopyOnWriteArrayList` preserves insertion order (the spec's "append-only write preserving insertion order") and is thread-safe under low-contention append + occasional read.

- [ ] **Step 2: Write `IngressClientConfig`**

```java
package com.canary.audit.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

@Configuration
public class IngressClientConfig {

    @Bean
    public RestClient ingressRestClient(
            RestClient.Builder builder,
            @Value("${app.restate.ingress.url}") String ingressUrl
    ) {
        return builder.baseUrl(ingressUrl).build();
    }
}
```

`RestClient.Builder` is auto-configured by Spring Boot 4 and lib-java's `XCanaryAutoConfiguration` already registers a `Consumer<RestClient.Builder>` that attaches `XCanaryRestClientInterceptor`. So this RestClient automatically stamps `x-canary` on every outbound call when the request thread is in canary context.

- [ ] **Step 3: Write `AuditController`**

```java
package com.canary.audit.controller;

import com.canary.audit.store.AuditEventStore;
import com.canary.restate.audit.AuditEvent;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestClient;

import java.util.List;

@RestController
public class AuditController {

    private final RestClient ingressClient;
    private final AuditEventStore store;

    public AuditController(RestClient ingressClient, AuditEventStore store) {
        this.ingressClient = ingressClient;
        this.store = store;
    }

    @PostMapping("/audit/events")
    public ResponseEntity<Void> create(@RequestBody AuditEvent event) {
        ingressClient.post()
            .uri("/AuditQueryService/append")
            .body(event)
            .retrieve()
            .toBodilessEntity();
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @GetMapping("/audit/by-aggregate/{id}")
    public List<AuditEvent> byAggregate(@PathVariable("id") String id) {
        return store.findByAggregate(id);
    }
}
```

POST delegates to the Restate handler via Ingress (option β). GET is read-only — it reads the in-memory store directly (no need to round-trip Restate for queries).

- [ ] **Step 4: Write the failing controller test**

`services/audit-service/src/test/java/com/canary/audit/controller/AuditControllerTest.java`:

```java
package com.canary.audit.controller;

import com.canary.audit.store.AuditEventStore;
import com.canary.restate.audit.AuditEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.client.RestClient;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(AuditController.class)
class AuditControllerTest {

    @Autowired MockMvc mockMvc;
    @Autowired ObjectMapper objectMapper;

    @MockitoBean RestClient ingressClient;
    @MockitoBean AuditEventStore store;

    @Test
    void postDelegatesToRestateIngress() throws Exception {
        var event = new AuditEvent("payment", "ch_1", "charged", "ord_1");

        var uriSpec = mock(RestClient.RequestBodyUriSpec.class);
        var bodySpec = mock(RestClient.RequestBodySpec.class);
        var responseSpec = mock(RestClient.ResponseSpec.class);
        when(ingressClient.post()).thenReturn(uriSpec);
        when(uriSpec.uri("/AuditQueryService/append")).thenReturn(bodySpec);
        when(bodySpec.body(any(AuditEvent.class))).thenReturn(bodySpec);
        when(bodySpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.toBodilessEntity()).thenReturn(ResponseEntity.ok().build());

        mockMvc.perform(post("/audit/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(event)))
            .andExpect(status().isCreated());

        var captor = ArgumentCaptor.forClass(AuditEvent.class);
        verify(bodySpec).body(captor.capture());
        assertThat(captor.getValue()).isEqualTo(event);
    }

    @Test
    void getByAggregateReadsStoreDirectly() throws Exception {
        var event = new AuditEvent("ord_1", "evt_1", "created", null);
        when(store.findByAggregate("ord_1")).thenReturn(List.of(event));

        mockMvc.perform(get("/audit/by-aggregate/ord_1"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$[0].id").value("evt_1"))
            .andExpect(jsonPath("$[0].aggregate").value("ord_1"));
    }
}
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `./gradlew :services:audit-service:test --tests AuditControllerTest --quiet`

Expected: 2 tests pass.

- [ ] **Step 6: Run full module tests to verify smoke test still passes**

Run: `./gradlew :services:audit-service:test --quiet`

Expected: 3 tests pass (smoke + 2 controller).

- [ ] **Step 7: Commit**

```bash
git add services/audit-service/
git commit -m "feat(audit-service): AuditController delegates to Restate Ingress + reads store for GET"
```

---

### Task 6: AuditQueryServiceImpl Restate handler + endpoint config (gated)

**Files:**
- Create: `services/audit-service/src/main/java/com/canary/audit/handler/AuditQueryServiceImpl.java`
- Create: `services/audit-service/src/main/java/com/canary/audit/config/RestateEndpointConfig.java`
- Create: `services/audit-service/src/test/java/com/canary/audit/handler/AuditQueryServiceImplTest.java`
- Create: `services/audit-service/src/test/java/com/canary/audit/config/RestateEndpointGatingTest.java`

- [ ] **Step 1: Write `AuditQueryServiceImpl`** (handler — Kafka emission added in Task 7)

```java
package com.canary.audit.handler;

import com.canary.audit.store.AuditEventStore;
import com.canary.restate.audit.AuditEvent;
import com.canary.restate.audit.AuditQueryService;
import dev.restate.sdk.Context;

import java.util.List;

public class AuditQueryServiceImpl extends AuditQueryService {

    private final AuditEventStore store;

    public AuditQueryServiceImpl(AuditEventStore store) {
        this.store = store;
    }

    @Override
    public void append(Context ctx, AuditEvent event) {
        store.append(event);
        // Kafka emission added in Task 7.
    }

    @Override
    public List<AuditEvent> byAggregate(Context ctx, String aggregateId) {
        return store.findByAggregate(aggregateId);
    }
}
```

- [ ] **Step 2: Write the handler unit test**

`services/audit-service/src/test/java/com/canary/audit/handler/AuditQueryServiceImplTest.java`:

```java
package com.canary.audit.handler;

import com.canary.audit.store.AuditEventStore;
import com.canary.restate.audit.AuditEvent;
import dev.restate.sdk.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class AuditQueryServiceImplTest {

    AuditEventStore store;
    AuditQueryServiceImpl handler;
    Context ctx;

    @BeforeEach
    void setUp() {
        store = new AuditEventStore();
        handler = new AuditQueryServiceImpl(store);
        ctx = mock(Context.class);
    }

    @Test
    void appendStoresTheEvent() {
        var event = new AuditEvent("ord_1", "evt_1", "created", null);

        handler.append(ctx, event);

        assertThat(store.all()).containsExactly(event);
    }

    @Test
    void byAggregateFiltersByAggregateField() {
        store.append(new AuditEvent("ord_1", "e1", "x", null));
        store.append(new AuditEvent("ord_2", "e2", "x", null));
        store.append(new AuditEvent("ord_1", "e3", "x", null));

        List<AuditEvent> result = handler.byAggregate(ctx, "ord_1");

        assertThat(result).extracting(AuditEvent::id).containsExactly("e1", "e3");
    }

    @Test
    void byAggregatePreservesInsertionOrder() {
        for (int i = 0; i < 5; i++) {
            store.append(new AuditEvent("a", "e" + i, "x", null));
        }

        List<AuditEvent> result = handler.byAggregate(ctx, "a");

        assertThat(result).extracting(AuditEvent::id).containsExactly("e0", "e1", "e2", "e3", "e4");
    }
}
```

- [ ] **Step 3: Write `RestateEndpointConfig`** (gated by flag)

```java
package com.canary.audit.config;

import com.canary.audit.handler.AuditQueryServiceImpl;
import com.canary.audit.store.AuditEventStore;
import dev.restate.sdk.endpoint.Endpoint;
import dev.restate.sdk.http.vertx.RestateHttpServer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(
    name = "app.restate.register-handlers",
    havingValue = "true",
    matchIfMissing = true
)
public class RestateEndpointConfig {

    private final AuditEventStore store;
    private final int port;
    private RestateHttpServer server;

    public RestateEndpointConfig(AuditEventStore store,
                                 @Value("${app.restate.handler.port}") int port) {
        this.store = store;
        this.port = port;
    }

    @Bean
    public AuditQueryServiceImpl auditQueryServiceImpl() {
        return new AuditQueryServiceImpl(store);
    }

    @PostConstruct
    void start() {
        server = RestateHttpServer.fromEndpoint(
            Endpoint.builder().bind(auditQueryServiceImpl()).build()
        );
        server.listen(port);
    }

    @PreDestroy
    void stop() throws Exception {
        if (server != null) {
            server.close();
        }
    }
}
```

**SDK API note:** the exact import path / builder method names for `RestateHttpServer` and `Endpoint.builder().bind(...)` depend on `dev.restate:sdk-http-vertx:2.7.0` — adjust to the actual artifact's API if different. The structural intent is: gate the bean on the flag; build an HTTP endpoint that binds the handler; start at construction; close cleanly on shutdown.

- [ ] **Step 4: Write the gating test**

`services/audit-service/src/test/java/com/canary/audit/config/RestateEndpointGatingTest.java`:

```java
package com.canary.audit.config;

import com.canary.audit.store.AuditEventStore;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

class RestateEndpointGatingTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withUserConfiguration(TestStubs.class, RestateEndpointConfig.class)
        .withPropertyValues("app.restate.handler.port=0");

    @Test
    void whenFlagTrueThenEndpointConfigIsActive() {
        runner.withPropertyValues("app.restate.register-handlers=true")
            .run(ctx -> assertThat(ctx).hasSingleBean(RestateEndpointConfig.class));
    }

    @Test
    void whenFlagFalseThenEndpointConfigIsSkipped() {
        runner.withPropertyValues("app.restate.register-handlers=false")
            .run(ctx -> assertThat(ctx).doesNotHaveBean(RestateEndpointConfig.class));
    }

    @Test
    void whenFlagAbsentThenDefaultsToActive() {
        runner.run(ctx -> assertThat(ctx).hasSingleBean(RestateEndpointConfig.class));
    }

    @Configuration
    static class TestStubs {
        @Bean
        AuditEventStore auditEventStore() {
            return new AuditEventStore();
        }
    }
}
```

This test does NOT actually start the embedded HTTP server — `@PostConstruct start()` will run and try to listen on port 0, which is acceptable (port 0 = "any free port"). If that proves fragile in the test runner, replace `start()` body with a no-op behind a `@Profile("!test")` gate or split the lifecycle into a separate bean.

- [ ] **Step 5: Run all audit-service tests**

Run: `./gradlew :services:audit-service:test --quiet`

Expected: 9+ tests pass (smoke + 2 controller + 3 handler + 3 gating). The gating tests may surface SDK-API issues — adjust imports as required by your SDK build.

- [ ] **Step 6: Commit**

```bash
git add services/audit-service/
git commit -m "feat(audit-service): AuditQueryService Restate handler + gated endpoint config"
```

---

### Task 7: Kafka producer + consumer (gated) + GET /internal/consumed-events

**Files:**
- Create: `services/audit-service/src/main/java/com/canary/audit/kafka/KafkaProducerConfig.java`
- Create: `services/audit-service/src/main/java/com/canary/audit/kafka/AuditKafkaListener.java`
- Create: `services/audit-service/src/main/java/com/canary/audit/store/ConsumedEvent.java`
- Create: `services/audit-service/src/main/java/com/canary/audit/store/ConsumedEventStore.java`
- Create: `services/audit-service/src/main/java/com/canary/audit/controller/InternalController.java`
- Modify: `services/audit-service/src/main/java/com/canary/audit/handler/AuditQueryServiceImpl.java` (add Kafka emission)
- Create: `services/audit-service/src/test/java/com/canary/audit/kafka/KafkaProducerConfigTest.java`
- Create: `services/audit-service/src/test/java/com/canary/audit/kafka/AuditKafkaListenerGatingTest.java`
- Create: `services/audit-service/src/test/java/com/canary/audit/controller/InternalControllerTest.java`
- Modify: `services/audit-service/src/test/java/com/canary/audit/handler/AuditQueryServiceImplTest.java` (Kafka assertion)

- [ ] **Step 1: Write `ConsumedEvent` + `ConsumedEventStore`**

`services/audit-service/src/main/java/com/canary/audit/store/ConsumedEvent.java`:

```java
package com.canary.audit.store;

import java.util.Map;

public record ConsumedEvent(String topic, String key, String value, Map<String, String> headers) {}
```

`services/audit-service/src/main/java/com/canary/audit/store/ConsumedEventStore.java`:

```java
package com.canary.audit.store;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class ConsumedEventStore {

    private final List<ConsumedEvent> events = new CopyOnWriteArrayList<>();

    public void record(ConsumedEvent event) {
        events.add(event);
    }

    public List<ConsumedEvent> all() {
        return new ArrayList<>(events);
    }
}
```

- [ ] **Step 2: Write `InternalController`** (`/internal/consumed-events`)

```java
package com.canary.audit.controller;

import com.canary.audit.store.ConsumedEvent;
import com.canary.audit.store.ConsumedEventStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class InternalController {

    private final ConsumedEventStore store;

    public InternalController(ConsumedEventStore store) {
        this.store = store;
    }

    @GetMapping("/internal/consumed-events")
    public List<ConsumedEvent> consumedEvents() {
        return store.all();
    }
}
```

- [ ] **Step 3: Write the failing `InternalController` test**

`services/audit-service/src/test/java/com/canary/audit/controller/InternalControllerTest.java`:

```java
package com.canary.audit.controller;

import com.canary.audit.store.ConsumedEvent;
import com.canary.audit.store.ConsumedEventStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(InternalController.class)
class InternalControllerTest {

    @Autowired MockMvc mockMvc;
    @MockitoBean ConsumedEventStore store;

    @Test
    void consumedEventsEndpointReturnsRecordedEvents() throws Exception {
        when(store.all()).thenReturn(List.of(
            new ConsumedEvent("orders.events", "ord_1", "{}", Map.of("x-canary", "true"))
        ));

        mockMvc.perform(get("/internal/consumed-events"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$[0].topic").value("orders.events"))
            .andExpect(jsonPath("$[0].key").value("ord_1"))
            .andExpect(jsonPath("$[0].headers['x-canary']").value("true"));
    }
}
```

Run: `./gradlew :services:audit-service:test --tests InternalControllerTest --quiet`

Expected: passes.

- [ ] **Step 4: Write `KafkaProducerConfig`**

```java
package com.canary.audit.kafka;

import com.canary.platform.lib.XCanaryKafkaProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, String> producerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers
    ) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, XCanaryKafkaProducerInterceptor.class.getName());
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> pf) {
        return new KafkaTemplate<>(pf);
    }
}
```

- [ ] **Step 5: Write the producer config test**

`services/audit-service/src/test/java/com/canary/audit/kafka/KafkaProducerConfigTest.java`:

```java
package com.canary.audit.kafka;

import com.canary.platform.lib.XCanaryKafkaProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaProducerConfigTest {

    @Test
    void producerFactoryHasXCanaryInterceptorConfigured() {
        var config = new KafkaProducerConfig();
        ProducerFactory<String, String> factory = config.producerFactory("localhost:9092");

        var props = ((DefaultKafkaProducerFactory<String, String>) factory).getConfigurationProperties();

        assertThat(props.get(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG))
            .isEqualTo(XCanaryKafkaProducerInterceptor.class.getName());
    }
}
```

- [ ] **Step 6: Modify `AuditQueryServiceImpl` to emit Kafka on `append`**

Update `services/audit-service/src/main/java/com/canary/audit/handler/AuditQueryServiceImpl.java`:

```java
package com.canary.audit.handler;

import com.canary.audit.store.AuditEventStore;
import com.canary.restate.audit.AuditEvent;
import com.canary.restate.audit.AuditQueryService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.restate.sdk.Context;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;

public class AuditQueryServiceImpl extends AuditQueryService {

    private final AuditEventStore store;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public AuditQueryServiceImpl(AuditEventStore store,
                                 KafkaTemplate<String, String> kafkaTemplate,
                                 ObjectMapper objectMapper) {
        this.store = store;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    public void append(Context ctx, AuditEvent event) {
        store.append(event);
        try {
            String json = objectMapper.writeValueAsString(event);
            kafkaTemplate.send("audit.events", event.id(), json);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize AuditEvent", e);
        }
    }

    @Override
    public List<AuditEvent> byAggregate(Context ctx, String aggregateId) {
        return store.findByAggregate(aggregateId);
    }
}
```

- [ ] **Step 7: Update the handler test to assert Kafka emission**

Replace `services/audit-service/src/test/java/com/canary/audit/handler/AuditQueryServiceImplTest.java` with:

```java
package com.canary.audit.handler;

import com.canary.audit.store.AuditEventStore;
import com.canary.restate.audit.AuditEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.restate.sdk.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class AuditQueryServiceImplTest {

    AuditEventStore store;
    @SuppressWarnings("unchecked")
    KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    ObjectMapper objectMapper = new ObjectMapper();
    AuditQueryServiceImpl handler;
    Context ctx = mock(Context.class);

    @BeforeEach
    void setUp() {
        store = new AuditEventStore();
        handler = new AuditQueryServiceImpl(store, kafkaTemplate, objectMapper);
    }

    @Test
    void appendStoresAndEmitsKafka() throws Exception {
        var event = new AuditEvent("ord_1", "evt_1", "created", null);

        handler.append(ctx, event);

        assertThat(store.all()).containsExactly(event);

        var keyCap = ArgumentCaptor.forClass(String.class);
        var valueCap = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq("audit.events"), keyCap.capture(), valueCap.capture());
        assertThat(keyCap.getValue()).isEqualTo("evt_1");
        assertThat(objectMapper.readValue(valueCap.getValue(), AuditEvent.class)).isEqualTo(event);
    }

    @Test
    void byAggregateFiltersByAggregateField() {
        store.append(new AuditEvent("ord_1", "e1", "x", null));
        store.append(new AuditEvent("ord_2", "e2", "x", null));
        store.append(new AuditEvent("ord_1", "e3", "x", null));

        List<AuditEvent> result = handler.byAggregate(ctx, "ord_1");

        assertThat(result).extracting(AuditEvent::id).containsExactly("e1", "e3");
    }

    @Test
    void byAggregatePreservesInsertionOrder() {
        for (int i = 0; i < 5; i++) {
            store.append(new AuditEvent("a", "e" + i, "x", null));
        }

        List<AuditEvent> result = handler.byAggregate(ctx, "a");

        assertThat(result).extracting(AuditEvent::id).containsExactly("e0", "e1", "e2", "e3", "e4");
    }
}
```

- [ ] **Step 8: Update `RestateEndpointConfig` to wire the new dependencies into `AuditQueryServiceImpl`**

Update the bean method in `services/audit-service/src/main/java/com/canary/audit/config/RestateEndpointConfig.java`:

```java
@Bean
public AuditQueryServiceImpl auditQueryServiceImpl(
        AuditEventStore store,
        KafkaTemplate<String, String> kafkaTemplate,
        ObjectMapper objectMapper
) {
    return new AuditQueryServiceImpl(store, kafkaTemplate, objectMapper);
}
```

Add the imports:

```java
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
```

Also remove the now-unused field `private final AuditEventStore store;` and constructor parameter, OR rewire the constructor to delegate stores from the bean — preserve only the lifecycle (port + server) state on the config class itself. Final config class:

```java
package com.canary.audit.config;

import com.canary.audit.handler.AuditQueryServiceImpl;
import com.canary.audit.store.AuditEventStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.restate.sdk.endpoint.Endpoint;
import dev.restate.sdk.http.vertx.RestateHttpServer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration
@ConditionalOnProperty(
    name = "app.restate.register-handlers",
    havingValue = "true",
    matchIfMissing = true
)
public class RestateEndpointConfig {

    private final int port;
    private final AuditQueryServiceImpl handler;
    private RestateHttpServer server;

    public RestateEndpointConfig(@Value("${app.restate.handler.port}") int port,
                                 AuditQueryServiceImpl handler) {
        this.port = port;
        this.handler = handler;
    }

    @Bean
    public static AuditQueryServiceImpl auditQueryServiceImpl(
            AuditEventStore store,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper
    ) {
        return new AuditQueryServiceImpl(store, kafkaTemplate, objectMapper);
    }

    @PostConstruct
    void start() {
        server = RestateHttpServer.fromEndpoint(
            Endpoint.builder().bind(handler).build()
        );
        server.listen(port);
    }

    @PreDestroy
    void stop() throws Exception {
        if (server != null) {
            server.close();
        }
    }
}
```

The static `@Bean` method allows Spring to construct the handler before the config class itself instantiates (the config depends on the bean it produces — chicken-and-egg fixed by `static`).

- [ ] **Step 8b: Update `RestateEndpointGatingTest` TestStubs to provide the new beans**

The static `@Bean auditQueryServiceImpl(...)` now requires `KafkaTemplate` and `ObjectMapper` — the gating test must provide them or the "flag=true" scenario fails to wire. Replace the `TestStubs` class in `services/audit-service/src/test/java/com/canary/audit/config/RestateEndpointGatingTest.java`:

```java
@Configuration
static class TestStubs {
    @Bean AuditEventStore auditEventStore() { return new AuditEventStore(); }
    @Bean
    @SuppressWarnings("unchecked")
    KafkaTemplate<String, String> kafkaTemplate() {
        return org.mockito.Mockito.mock(KafkaTemplate.class);
    }
    @Bean com.fasterxml.jackson.databind.ObjectMapper objectMapper() {
        return new com.fasterxml.jackson.databind.ObjectMapper();
    }
}
```

Add the import: `import org.springframework.kafka.core.KafkaTemplate;`.

- [ ] **Step 9: Write `AuditKafkaListener`** (consumes all `*.events` topics, gated by flag)

```java
package com.canary.audit.kafka;

import com.canary.audit.store.ConsumedEvent;
import com.canary.audit.store.ConsumedEventStore;
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

    public AuditKafkaListener(ConsumedEventStore store) {
        this.store = store;
    }

    @KafkaListener(
        topics = {"orders.events", "payments.events", "inventory.events", "notifications.events"},
        groupId = "audit-service"
    )
    public void onMessage(ConsumerRecord<String, String> record) {
        Map<String, String> headers = new HashMap<>();
        record.headers().forEach(h -> headers.put(h.key(), new String(h.value(), StandardCharsets.UTF_8)));
        store.record(new ConsumedEvent(record.topic(), record.key(), record.value(), headers));
    }
}
```

audit-svc consumes the four upstream topics (orders, payments, inventory, notifications) per the spec line 153 — but explicitly NOT `audit.events` (its own checkpoint topic). The listener is gated by `app.kafka.consumers.enabled`; canary pods set this false.

- [ ] **Step 10: Write the consumer gating test**

`services/audit-service/src/test/java/com/canary/audit/kafka/AuditKafkaListenerGatingTest.java`:

```java
package com.canary.audit.kafka;

import com.canary.audit.store.ConsumedEventStore;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

class AuditKafkaListenerGatingTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class))
        .withUserConfiguration(TestStubs.class, AuditKafkaListener.class)
        .withPropertyValues(
            "spring.kafka.bootstrap-servers=localhost:0"
        );

    @Test
    void whenFlagTrueThenListenerIsRegistered() {
        runner.withPropertyValues("app.kafka.consumers.enabled=true")
            .run(ctx -> assertThat(ctx).hasSingleBean(AuditKafkaListener.class));
    }

    @Test
    void whenFlagFalseThenListenerIsAbsent() {
        runner.withPropertyValues("app.kafka.consumers.enabled=false")
            .run(ctx -> assertThat(ctx).doesNotHaveBean(AuditKafkaListener.class));
    }

    @Test
    void whenFlagAbsentThenDefaultsToActive() {
        runner.run(ctx -> assertThat(ctx).hasSingleBean(AuditKafkaListener.class));
    }

    @Configuration
    static class TestStubs {
        @Bean
        ConsumedEventStore consumedEventStore() {
            return new ConsumedEventStore();
        }
    }
}
```

- [ ] **Step 11: Run all audit-service tests**

Run: `./gradlew :services:audit-service:test --quiet`

Expected: ~13 tests pass (smoke + 2 controller + 3 handler + 3 endpoint gating + 1 producer config + 3 consumer gating + 1 internal controller).

- [ ] **Step 12: Commit**

```bash
git add services/audit-service/
git commit -m "feat(audit-service): Kafka producer/consumer (gated) + /internal/consumed-events"
```

audit-service is now complete. Run `./gradlew :services:audit-service:test :platform:lib-java:test :platform:restate-defs-java:compileJava --quiet` to verify the full Java side compiles + tests pass before moving on.

---

## Phase C — payment-service (Java, `@VirtualObject`)

payment-service has the most interesting Restate handler in 1.3.a — `PaymentVO` is a `@VirtualObject` keyed by `orderId` with idempotency via Restate state, and it makes a Restate-to-Restate call to `AuditQueryService.append`.

### Task 8: Scaffold `services/payment-service` Gradle subproject

**Files:**
- Modify: `settings.gradle.kts`
- Create: `services/payment-service/build.gradle.kts`
- Create: `services/payment-service/src/main/resources/application.yml`
- Create: `services/payment-service/src/test/resources/application-test.yml`
- Create: `services/payment-service/src/main/java/com/canary/payment/PaymentApplication.java`
- Create: `services/payment-service/src/test/java/com/canary/payment/PaymentApplicationTest.java`

- [ ] **Step 1: Add `services:payment-service` to `settings.gradle.kts`**

Add the line:

```kotlin
include("services:payment-service")
```

- [ ] **Step 2: Write `services/payment-service/build.gradle.kts`**

```kotlin
plugins {
    java
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
}

dependencies {
    implementation(project(":platform:lib-java"))
    implementation(project(":platform:restate-defs-java"))
    implementation(libs.spring.boot.starter.web)
    implementation(libs.spring.kafka)
    implementation(libs.restate.sdk.api)
    implementation(libs.restate.sdk.common)
    implementation(libs.restate.sdk.http.vertx)

    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.mockito.core)
    testImplementation(libs.assertj.core)
    testRuntimeOnly(libs.junit.platform.launcher)
}
```

- [ ] **Step 3: Write `services/payment-service/src/main/resources/application.yml`**

```yaml
server:
  port: 8081

spring:
  application:
    name: payment-service
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}

app:
  kafka:
    consumers:
      enabled: ${KAFKA_CONSUMERS_ENABLED:true}
  restate:
    register-handlers: ${RESTATE_REGISTER_HANDLERS:true}
    ingress:
      url: ${RESTATE_INGRESS_URL:http://localhost:9070}
    handler:
      port: ${RESTATE_HANDLER_PORT:9081}
```

- [ ] **Step 4: Write `services/payment-service/src/test/resources/application-test.yml`**

```yaml
server:
  port: 0

spring:
  kafka:
    bootstrap-servers: localhost:0

app:
  kafka:
    consumers:
      enabled: false
  restate:
    register-handlers: false
    ingress:
      url: http://example.invalid
    handler:
      port: 0
```

- [ ] **Step 5: Write `PaymentApplication`**

`services/payment-service/src/main/java/com/canary/payment/PaymentApplication.java`:

```java
package com.canary.payment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class PaymentApplication {
    public static void main(String[] args) {
        SpringApplication.run(PaymentApplication.class, args);
    }
}
```

- [ ] **Step 6: Write the smoke test**

`services/payment-service/src/test/java/com/canary/payment/PaymentApplicationTest.java`:

```java
package com.canary.payment;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("test")
class PaymentApplicationTest {
    @Test
    void contextLoads() {}
}
```

- [ ] **Step 7: Run the test**

Run: `./gradlew :services:payment-service:test --quiet`

Expected: BUILD SUCCESSFUL, 1 test passes.

- [ ] **Step 8: Commit**

```bash
git add settings.gradle.kts services/payment-service/
git commit -m "feat(payment-service): scaffold Gradle subproject + smoke test"
```

---

### Task 9: ChargeController + ChargeStore + Ingress delegation

**Files:**
- Create: `services/payment-service/src/main/java/com/canary/payment/store/ChargeStore.java`
- Create: `services/payment-service/src/main/java/com/canary/payment/config/IngressClientConfig.java`
- Create: `services/payment-service/src/main/java/com/canary/payment/controller/ChargeController.java`
- Create: `services/payment-service/src/test/java/com/canary/payment/controller/ChargeControllerTest.java`

- [ ] **Step 1: Write `ChargeStore`**

```java
package com.canary.payment.store;

import com.canary.restate.payment.Charge;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class ChargeStore {

    private final ConcurrentMap<String, Charge> byId = new ConcurrentHashMap<>();

    public void put(Charge charge) {
        byId.put(charge.id(), charge);
    }

    public Optional<Charge> findById(String id) {
        return Optional.ofNullable(byId.get(id));
    }
}
```

- [ ] **Step 2: Write `IngressClientConfig`**

```java
package com.canary.payment.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

@Configuration
public class IngressClientConfig {

    @Bean
    public RestClient ingressRestClient(
            RestClient.Builder builder,
            @Value("${app.restate.ingress.url}") String ingressUrl
    ) {
        return builder.baseUrl(ingressUrl).build();
    }
}
```

- [ ] **Step 3: Write `ChargeController`**

```java
package com.canary.payment.controller;

import com.canary.payment.store.ChargeStore;
import com.canary.restate.payment.Charge;
import com.canary.restate.payment.ChargeRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestClient;

@RestController
public class ChargeController {

    private final RestClient ingressClient;
    private final ChargeStore store;

    public ChargeController(RestClient ingressClient, ChargeStore store) {
        this.ingressClient = ingressClient;
        this.store = store;
    }

    @PostMapping("/charges")
    public ResponseEntity<Charge> create(@RequestBody ChargeRequest req) {
        // VirtualObject is keyed by orderId; Restate Ingress URL: /PaymentVO/{key}/charge
        Charge charge = ingressClient.post()
            .uri("/PaymentVO/{key}/charge", req.orderId())
            .body(req)
            .retrieve()
            .body(Charge.class);
        return ResponseEntity.status(HttpStatus.CREATED).body(charge);
    }

    @GetMapping("/charges/{id}")
    public ResponseEntity<Charge> byId(@PathVariable("id") String id) {
        return store.findById(id)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }
}
```

- [ ] **Step 4: Write the failing controller test**

`services/payment-service/src/test/java/com/canary/payment/controller/ChargeControllerTest.java`:

```java
package com.canary.payment.controller;

import com.canary.payment.store.ChargeStore;
import com.canary.restate.payment.Charge;
import com.canary.restate.payment.ChargeRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.client.RestClient;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(ChargeController.class)
class ChargeControllerTest {

    @Autowired MockMvc mockMvc;
    @Autowired ObjectMapper objectMapper;

    @MockitoBean RestClient ingressClient;
    @MockitoBean ChargeStore store;

    @Test
    void postDelegatesToVirtualObjectViaIngress() throws Exception {
        var req = new ChargeRequest("ord_42", 1500L);
        var returnedCharge = new Charge("ch_1", "ord_42", 1500L, "succeeded");

        var uriSpec = mock(RestClient.RequestBodyUriSpec.class);
        var bodySpec = mock(RestClient.RequestBodySpec.class);
        var responseSpec = mock(RestClient.ResponseSpec.class);
        when(ingressClient.post()).thenReturn(uriSpec);
        when(uriSpec.uri(eq("/PaymentVO/{key}/charge"), eq("ord_42"))).thenReturn(bodySpec);
        when(bodySpec.body(any(ChargeRequest.class))).thenReturn(bodySpec);
        when(bodySpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.body(Charge.class)).thenReturn(returnedCharge);

        mockMvc.perform(post("/charges")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(req)))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.id").value("ch_1"))
            .andExpect(jsonPath("$.status").value("succeeded"));

        var captor = ArgumentCaptor.forClass(ChargeRequest.class);
        verify(bodySpec).body(captor.capture());
        assertThat(captor.getValue()).isEqualTo(req);
    }

    @Test
    void getByIdReturns200WhenFound() throws Exception {
        var charge = new Charge("ch_1", "ord_1", 100L, "succeeded");
        when(store.findById("ch_1")).thenReturn(Optional.of(charge));

        mockMvc.perform(get("/charges/ch_1"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.id").value("ch_1"));
    }

    @Test
    void getByIdReturns404WhenMissing() throws Exception {
        when(store.findById("nope")).thenReturn(Optional.empty());

        mockMvc.perform(get("/charges/nope"))
            .andExpect(status().isNotFound());
    }
}
```

- [ ] **Step 5: Run the test**

Run: `./gradlew :services:payment-service:test --tests ChargeControllerTest --quiet`

Expected: 3 tests pass.

- [ ] **Step 6: Run all payment-service tests**

Run: `./gradlew :services:payment-service:test --quiet`

Expected: 4 tests pass (smoke + 3 controller).

- [ ] **Step 7: Commit**

```bash
git add services/payment-service/
git commit -m "feat(payment-service): ChargeController delegates to PaymentVO via Ingress"
```

---

### Task 10: PaymentVOImpl Restate handler + endpoint config (gated)

The handler uses Restate state (`StateKey<Charge>`) for idempotency-by-orderId AND writes to the `ChargeStore` so HTTP `GET /charges/{id}` can read by chargeId. Calls `AuditQueryService.append` via Restate-to-Restate with the customizer-stamped options.

**Files:**
- Create: `services/payment-service/src/main/java/com/canary/payment/handler/PaymentVOImpl.java`
- Create: `services/payment-service/src/main/java/com/canary/payment/config/RestateEndpointConfig.java`
- Create: `services/payment-service/src/test/java/com/canary/payment/handler/PaymentVOImplTest.java`
- Create: `services/payment-service/src/test/java/com/canary/payment/config/RestateEndpointGatingTest.java`

- [ ] **Step 1: Write `PaymentVOImpl`** (Restate handler — Kafka emission added in Task 11)

```java
package com.canary.payment.handler;

import com.canary.payment.store.ChargeStore;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import com.canary.restate.audit.AuditEvent;
import com.canary.restate.audit.AuditQueryService;
import com.canary.restate.payment.Charge;
import com.canary.restate.payment.ChargeRequest;
import com.canary.restate.payment.PaymentVO;
import dev.restate.common.InvocationOptions;
import dev.restate.sdk.ObjectContext;
import dev.restate.serde.jackson.JacksonSerdeFactory;
import dev.restate.sdk.common.StateKey;

import java.util.Optional;
import java.util.UUID;

public class PaymentVOImpl extends PaymentVO {

    private static final StateKey<Charge> CHARGE_STATE =
        StateKey.of("charge", JacksonSerdeFactory.create(Charge.class));

    private final ChargeStore store;
    private final XCanaryRestateClientCustomizer canary;

    public PaymentVOImpl(ChargeStore store, XCanaryRestateClientCustomizer canary) {
        this.store = store;
        this.canary = canary;
    }

    @Override
    public Charge charge(ObjectContext ctx, ChargeRequest req) {
        // Idempotency: same orderId → same VO instance → same Restate state.
        Optional<Charge> existing = ctx.get(CHARGE_STATE);
        if (existing.isPresent()) {
            return existing.get();
        }

        Charge charge = new Charge(
            UUID.randomUUID().toString(),
            req.orderId(),
            req.amount(),
            "succeeded"
        );
        ctx.set(CHARGE_STATE, charge);
        store.put(charge);

        // Restate-to-Restate: append to audit. Customizer stamps x-canary on opts.
        InvocationOptions opts = canary.apply(InvocationOptions.builder());
        ctx.serviceClient(AuditQueryService.class)
           .call(AuditQueryService::append,
                 new AuditEvent("payment", charge.id(), "charged", req.orderId()),
                 opts);

        return charge;
    }
}
```

**SDK API note:** `StateKey.of(name, serde)` and `JacksonSerdeFactory.create(Class)` are SDK 2.x patterns. If your SDK exposes a simpler `StateKey.of(name, Charge.class)` shorthand, use it. The `ctx.serviceClient(C.class).call(C::method, input, opts)` shape comes directly from `XCanaryRestateClientCustomizer`'s Javadoc (lib-java) — keep this surface even if the imports differ.

- [ ] **Step 2: Write the failing handler unit test**

`services/payment-service/src/test/java/com/canary/payment/handler/PaymentVOImplTest.java`:

```java
package com.canary.payment.handler;

import com.canary.payment.store.ChargeStore;
import com.canary.platform.lib.XCanaryContext;
import com.canary.platform.lib.XCanaryConstants;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import com.canary.restate.audit.AuditEvent;
import com.canary.restate.audit.AuditQueryService;
import com.canary.restate.payment.Charge;
import com.canary.restate.payment.ChargeRequest;
import dev.restate.common.InvocationOptions;
import dev.restate.sdk.ObjectContext;
import dev.restate.sdk.common.StateKey;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class PaymentVOImplTest {

    ChargeStore store;
    XCanaryRestateClientCustomizer canary;
    PaymentVOImpl handler;
    ObjectContext ctx;
    AuditQueryServiceClientStub auditClient; // proxy returned by ctx.serviceClient

    @BeforeEach
    void setUp() {
        store = new ChargeStore();
        canary = new XCanaryRestateClientCustomizer();
        handler = new PaymentVOImpl(store, canary);
        ctx = mock(ObjectContext.class);
        auditClient = mock(AuditQueryServiceClientStub.class);
        when(ctx.serviceClient(eq(AuditQueryService.class))).thenAnswer(inv -> auditClient);
    }

    @AfterEach
    void clearCanary() {
        XCanaryContext.clear();
    }

    @Test
    void chargeReturnsExistingWhenStateAlreadySet() {
        Charge existing = new Charge("ch_existing", "ord_1", 999L, "succeeded");
        when(ctx.get(any(StateKey.class))).thenReturn(Optional.of(existing));

        Charge result = handler.charge(ctx, new ChargeRequest("ord_1", 999L));

        assertThat(result).isEqualTo(existing);
        assertThat(store.findById("ch_existing")).isEmpty(); // didn't re-write to store
        verifyNoInteractions(auditClient);
    }

    @Test
    void chargeWritesStateAndStoreAndCallsAuditWhenStateAbsent() {
        when(ctx.get(any(StateKey.class))).thenReturn(Optional.empty());

        Charge result = handler.charge(ctx, new ChargeRequest("ord_42", 1500L));

        assertThat(result.orderId()).isEqualTo("ord_42");
        assertThat(result.amount()).isEqualTo(1500L);
        assertThat(result.status()).isEqualTo("succeeded");
        assertThat(store.findById(result.id())).contains(result);

        verify(ctx).set(any(StateKey.class), eq(result));

        var auditCap = ArgumentCaptor.forClass(AuditEvent.class);
        verify(auditClient).call(any(), auditCap.capture(), any(InvocationOptions.class));
        AuditEvent emitted = auditCap.getValue();
        assertThat(emitted.aggregate()).isEqualTo("payment");
        assertThat(emitted.id()).isEqualTo(result.id());
        assertThat(emitted.action()).isEqualTo("charged");
        assertThat(emitted.correlationId()).isEqualTo("ord_42");
    }

    @Test
    void chargeStampsXCanaryOnAuditCallWhenContextIsCanary() {
        XCanaryContext.set(true);
        when(ctx.get(any(StateKey.class))).thenReturn(Optional.empty());

        handler.charge(ctx, new ChargeRequest("ord_1", 100L));

        var optsCap = ArgumentCaptor.forClass(InvocationOptions.class);
        verify(auditClient).call(any(), any(AuditEvent.class), optsCap.capture());
        assertThat(optsCap.getValue().getHeaders())
            .containsEntry(XCanaryConstants.HEADER_NAME, XCanaryConstants.TRUE_VALUE);
    }

    /** Minimal proxy interface matching what {@code ctx.serviceClient(AuditQueryService.class)} returns. */
    interface AuditQueryServiceClientStub {
        void call(java.util.function.BiConsumer<AuditQueryService, AuditEvent> ref,
                  AuditEvent input,
                  InvocationOptions opts);
    }
}
```

**Test note:** `ctx.serviceClient(C.class)` returns an SDK-generated client with a typed `.call(...)` method. The `AuditQueryServiceClientStub` interface in the test is a minimal stand-in — adjust its signature to match the SDK-generated client's exact `.call()` signature in your build (commonly `.call(MethodReference, input)` or `.call(MethodReference, input, InvocationOptions)`). The intent: verify the handler calls `serviceClient(AuditQueryService.class).call(...)` with the right `AuditEvent` payload and the canary-stamped `InvocationOptions`.

- [ ] **Step 3: Write `RestateEndpointConfig`** (gated by flag)

```java
package com.canary.payment.config;

import com.canary.payment.handler.PaymentVOImpl;
import com.canary.payment.store.ChargeStore;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import dev.restate.sdk.endpoint.Endpoint;
import dev.restate.sdk.http.vertx.RestateHttpServer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(
    name = "app.restate.register-handlers",
    havingValue = "true",
    matchIfMissing = true
)
public class RestateEndpointConfig {

    private final int port;
    private final PaymentVOImpl handler;
    private RestateHttpServer server;

    public RestateEndpointConfig(@Value("${app.restate.handler.port}") int port,
                                 PaymentVOImpl handler) {
        this.port = port;
        this.handler = handler;
    }

    @Bean
    public static PaymentVOImpl paymentVOImpl(ChargeStore store,
                                              XCanaryRestateClientCustomizer canary) {
        return new PaymentVOImpl(store, canary);
    }

    @PostConstruct
    void start() {
        server = RestateHttpServer.fromEndpoint(
            Endpoint.builder().bind(handler).build()
        );
        server.listen(port);
    }

    @PreDestroy
    void stop() throws Exception {
        if (server != null) {
            server.close();
        }
    }
}
```

- [ ] **Step 4: Write the gating test**

`services/payment-service/src/test/java/com/canary/payment/config/RestateEndpointGatingTest.java`:

```java
package com.canary.payment.config;

import com.canary.payment.store.ChargeStore;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

class RestateEndpointGatingTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withUserConfiguration(TestStubs.class, RestateEndpointConfig.class)
        .withPropertyValues("app.restate.handler.port=0");

    @Test
    void whenFlagTrueThenEndpointConfigIsActive() {
        runner.withPropertyValues("app.restate.register-handlers=true")
            .run(ctx -> assertThat(ctx).hasSingleBean(RestateEndpointConfig.class));
    }

    @Test
    void whenFlagFalseThenEndpointConfigIsSkipped() {
        runner.withPropertyValues("app.restate.register-handlers=false")
            .run(ctx -> assertThat(ctx).doesNotHaveBean(RestateEndpointConfig.class));
    }

    @Test
    void whenFlagAbsentThenDefaultsToActive() {
        runner.run(ctx -> assertThat(ctx).hasSingleBean(RestateEndpointConfig.class));
    }

    @Configuration
    static class TestStubs {
        @Bean ChargeStore chargeStore() { return new ChargeStore(); }
        @Bean XCanaryRestateClientCustomizer canary() { return new XCanaryRestateClientCustomizer(); }
    }
}
```

- [ ] **Step 5: Run all payment-service tests**

Run: `./gradlew :services:payment-service:test --quiet`

Expected: ~10 tests pass (smoke + 3 controller + 3 handler + 3 gating).

- [ ] **Step 6: Commit**

```bash
git add services/payment-service/
git commit -m "feat(payment-service): PaymentVO Restate handler + idempotency + audit R-to-R"
```

---

### Task 11: Kafka producer (`payments.events`) + consumer (`orders.events`, gated) + `/internal/consumed-events`

**Files:**
- Create: `services/payment-service/src/main/java/com/canary/payment/kafka/KafkaProducerConfig.java`
- Create: `services/payment-service/src/main/java/com/canary/payment/kafka/PaymentKafkaListener.java`
- Create: `services/payment-service/src/main/java/com/canary/payment/store/ConsumedEvent.java`
- Create: `services/payment-service/src/main/java/com/canary/payment/store/ConsumedEventStore.java`
- Create: `services/payment-service/src/main/java/com/canary/payment/controller/InternalController.java`
- Modify: `services/payment-service/src/main/java/com/canary/payment/handler/PaymentVOImpl.java` (Kafka emission)
- Modify: `services/payment-service/src/main/java/com/canary/payment/config/RestateEndpointConfig.java` (wire KafkaTemplate + ObjectMapper)
- Modify: `services/payment-service/src/test/java/com/canary/payment/handler/PaymentVOImplTest.java` (Kafka assertion)
- Create: `services/payment-service/src/test/java/com/canary/payment/kafka/KafkaProducerConfigTest.java`
- Create: `services/payment-service/src/test/java/com/canary/payment/kafka/PaymentKafkaListenerGatingTest.java`
- Create: `services/payment-service/src/test/java/com/canary/payment/controller/InternalControllerTest.java`

- [ ] **Step 1: Write `ConsumedEvent` + `ConsumedEventStore`**

`services/payment-service/src/main/java/com/canary/payment/store/ConsumedEvent.java`:

```java
package com.canary.payment.store;

import java.util.Map;

public record ConsumedEvent(String topic, String key, String value, Map<String, String> headers) {}
```

`services/payment-service/src/main/java/com/canary/payment/store/ConsumedEventStore.java`:

```java
package com.canary.payment.store;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class ConsumedEventStore {

    private final List<ConsumedEvent> events = new CopyOnWriteArrayList<>();

    public void record(ConsumedEvent event) {
        events.add(event);
    }

    public List<ConsumedEvent> all() {
        return new ArrayList<>(events);
    }
}
```

- [ ] **Step 2: Write `InternalController`**

```java
package com.canary.payment.controller;

import com.canary.payment.store.ConsumedEvent;
import com.canary.payment.store.ConsumedEventStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class InternalController {

    private final ConsumedEventStore store;

    public InternalController(ConsumedEventStore store) {
        this.store = store;
    }

    @GetMapping("/internal/consumed-events")
    public List<ConsumedEvent> consumedEvents() {
        return store.all();
    }
}
```

- [ ] **Step 3: Write the `InternalController` test**

`services/payment-service/src/test/java/com/canary/payment/controller/InternalControllerTest.java`:

```java
package com.canary.payment.controller;

import com.canary.payment.store.ConsumedEvent;
import com.canary.payment.store.ConsumedEventStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(InternalController.class)
class InternalControllerTest {

    @Autowired MockMvc mockMvc;
    @MockitoBean ConsumedEventStore store;

    @Test
    void consumedEventsEndpointReturnsRecordedEvents() throws Exception {
        when(store.all()).thenReturn(List.of(
            new ConsumedEvent("orders.events", "ord_1", "{}", Map.of("x-canary", "true"))
        ));

        mockMvc.perform(get("/internal/consumed-events"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$[0].topic").value("orders.events"))
            .andExpect(jsonPath("$[0].headers['x-canary']").value("true"));
    }
}
```

- [ ] **Step 4: Write `KafkaProducerConfig`**

```java
package com.canary.payment.kafka;

import com.canary.platform.lib.XCanaryKafkaProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, String> producerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers
    ) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, XCanaryKafkaProducerInterceptor.class.getName());
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> pf) {
        return new KafkaTemplate<>(pf);
    }
}
```

- [ ] **Step 5: Write the producer config test**

`services/payment-service/src/test/java/com/canary/payment/kafka/KafkaProducerConfigTest.java`:

```java
package com.canary.payment.kafka;

import com.canary.platform.lib.XCanaryKafkaProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaProducerConfigTest {

    @Test
    void producerFactoryHasXCanaryInterceptorConfigured() {
        var config = new KafkaProducerConfig();
        ProducerFactory<String, String> factory = config.producerFactory("localhost:9092");

        var props = ((DefaultKafkaProducerFactory<String, String>) factory).getConfigurationProperties();

        assertThat(props.get(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG))
            .isEqualTo(XCanaryKafkaProducerInterceptor.class.getName());
    }
}
```

- [ ] **Step 6: Update `PaymentVOImpl` to emit `payments.events`**

```java
package com.canary.payment.handler;

import com.canary.payment.store.ChargeStore;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import com.canary.restate.audit.AuditEvent;
import com.canary.restate.audit.AuditQueryService;
import com.canary.restate.payment.Charge;
import com.canary.restate.payment.ChargeRequest;
import com.canary.restate.payment.PaymentVO;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.restate.common.InvocationOptions;
import dev.restate.sdk.ObjectContext;
import dev.restate.sdk.common.StateKey;
import dev.restate.serde.jackson.JacksonSerdeFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Optional;
import java.util.UUID;

public class PaymentVOImpl extends PaymentVO {

    private static final StateKey<Charge> CHARGE_STATE =
        StateKey.of("charge", JacksonSerdeFactory.create(Charge.class));

    private final ChargeStore store;
    private final XCanaryRestateClientCustomizer canary;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public PaymentVOImpl(ChargeStore store,
                         XCanaryRestateClientCustomizer canary,
                         KafkaTemplate<String, String> kafkaTemplate,
                         ObjectMapper objectMapper) {
        this.store = store;
        this.canary = canary;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    public Charge charge(ObjectContext ctx, ChargeRequest req) {
        Optional<Charge> existing = ctx.get(CHARGE_STATE);
        if (existing.isPresent()) {
            return existing.get();
        }

        Charge charge = new Charge(
            UUID.randomUUID().toString(),
            req.orderId(),
            req.amount(),
            "succeeded"
        );
        ctx.set(CHARGE_STATE, charge);
        store.put(charge);

        try {
            kafkaTemplate.send("payments.events", charge.id(), objectMapper.writeValueAsString(charge));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize Charge", e);
        }

        InvocationOptions opts = canary.apply(InvocationOptions.builder());
        ctx.serviceClient(AuditQueryService.class)
           .call(AuditQueryService::append,
                 new AuditEvent("payment", charge.id(), "charged", req.orderId()),
                 opts);

        return charge;
    }
}
```

- [ ] **Step 7: Update `RestateEndpointConfig`'s static `@Bean` to wire the new dependencies**

Replace the static bean method in `services/payment-service/src/main/java/com/canary/payment/config/RestateEndpointConfig.java`:

```java
@Bean
public static PaymentVOImpl paymentVOImpl(ChargeStore store,
                                          XCanaryRestateClientCustomizer canary,
                                          KafkaTemplate<String, String> kafkaTemplate,
                                          com.fasterxml.jackson.databind.ObjectMapper objectMapper) {
    return new PaymentVOImpl(store, canary, kafkaTemplate, objectMapper);
}
```

Add the import:

```java
import org.springframework.kafka.core.KafkaTemplate;
```

- [ ] **Step 8: Update the gating-test `TestStubs` to provide the new beans**

```java
@Configuration
static class TestStubs {
    @Bean ChargeStore chargeStore() { return new ChargeStore(); }
    @Bean XCanaryRestateClientCustomizer canary() { return new XCanaryRestateClientCustomizer(); }
    @Bean
    @SuppressWarnings("unchecked")
    KafkaTemplate<String, String> kafkaTemplate() {
        return org.mockito.Mockito.mock(KafkaTemplate.class);
    }
    @Bean com.fasterxml.jackson.databind.ObjectMapper objectMapper() {
        return new com.fasterxml.jackson.databind.ObjectMapper();
    }
}
```

- [ ] **Step 9: Update `PaymentVOImplTest` constructor + add Kafka assertion**

In `services/payment-service/src/test/java/com/canary/payment/handler/PaymentVOImplTest.java`:

```java
// Add fields
KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
ObjectMapper objectMapper = new ObjectMapper();

// In setUp, swap to:
handler = new PaymentVOImpl(store, canary, kafkaTemplate, objectMapper);
```

Add a new test method asserting Kafka emission:

```java
@Test
void chargeEmitsPaymentsEvent() throws Exception {
    when(ctx.get(any(StateKey.class))).thenReturn(Optional.empty());

    Charge result = handler.charge(ctx, new ChargeRequest("ord_42", 1500L));

    var keyCap = ArgumentCaptor.forClass(String.class);
    var valueCap = ArgumentCaptor.forClass(String.class);
    verify(kafkaTemplate).send(eq("payments.events"), keyCap.capture(), valueCap.capture());
    assertThat(keyCap.getValue()).isEqualTo(result.id());
    assertThat(objectMapper.readValue(valueCap.getValue(), Charge.class)).isEqualTo(result);
}
```

Imports added:

```java
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
```

- [ ] **Step 10: Write `PaymentKafkaListener`** (consumes `orders.events` only)

```java
package com.canary.payment.kafka;

import com.canary.payment.store.ConsumedEvent;
import com.canary.payment.store.ConsumedEventStore;
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

    public PaymentKafkaListener(ConsumedEventStore store) {
        this.store = store;
    }

    @KafkaListener(topics = "orders.events", groupId = "payment-service")
    public void onMessage(ConsumerRecord<String, String> record) {
        Map<String, String> headers = new HashMap<>();
        record.headers().forEach(h -> headers.put(h.key(), new String(h.value(), StandardCharsets.UTF_8)));
        store.record(new ConsumedEvent(record.topic(), record.key(), record.value(), headers));
    }
}
```

- [ ] **Step 11: Write the consumer gating test**

`services/payment-service/src/test/java/com/canary/payment/kafka/PaymentKafkaListenerGatingTest.java`:

```java
package com.canary.payment.kafka;

import com.canary.payment.store.ConsumedEventStore;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

class PaymentKafkaListenerGatingTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class))
        .withUserConfiguration(TestStubs.class, PaymentKafkaListener.class)
        .withPropertyValues("spring.kafka.bootstrap-servers=localhost:0");

    @Test
    void whenFlagTrueThenListenerIsRegistered() {
        runner.withPropertyValues("app.kafka.consumers.enabled=true")
            .run(ctx -> assertThat(ctx).hasSingleBean(PaymentKafkaListener.class));
    }

    @Test
    void whenFlagFalseThenListenerIsAbsent() {
        runner.withPropertyValues("app.kafka.consumers.enabled=false")
            .run(ctx -> assertThat(ctx).doesNotHaveBean(PaymentKafkaListener.class));
    }

    @Test
    void whenFlagAbsentThenDefaultsToActive() {
        runner.run(ctx -> assertThat(ctx).hasSingleBean(PaymentKafkaListener.class));
    }

    @Configuration
    static class TestStubs {
        @Bean ConsumedEventStore consumedEventStore() { return new ConsumedEventStore(); }
    }
}
```

- [ ] **Step 12: Run all payment-service tests**

Run: `./gradlew :services:payment-service:test --quiet`

Expected: ~14 tests pass.

- [ ] **Step 13: Commit**

```bash
git add services/payment-service/
git commit -m "feat(payment-service): Kafka producer/consumer (gated) + payments.events emission + /internal/consumed-events"
```

---

## Phase D — inventory-service (Java, `@Workflow`)

inventory-service follows the same shape as payment-service. Differences: `ReservationWorkflow` is `@Workflow` (no `StateKey` idempotency in 1.3.a per trimmed-C); GET `/products/{sku}/availability` returns `100 - sum(reservations.quantity for sku)` from the in-memory store; handler emits `inventory.events`; consumer subscribes to `orders.events` only.

### Task 12: Scaffold `services/inventory-service` Gradle subproject

**Files:**
- Modify: `settings.gradle.kts`
- Create: `services/inventory-service/build.gradle.kts`
- Create: `services/inventory-service/src/main/resources/application.yml`
- Create: `services/inventory-service/src/test/resources/application-test.yml`
- Create: `services/inventory-service/src/main/java/com/canary/inventory/InventoryApplication.java`
- Create: `services/inventory-service/src/test/java/com/canary/inventory/InventoryApplicationTest.java`

- [ ] **Step 1: Add `services:inventory-service` to `settings.gradle.kts`**

```kotlin
include("services:inventory-service")
```

- [ ] **Step 2: Write `services/inventory-service/build.gradle.kts`**

```kotlin
plugins {
    java
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
}

dependencies {
    implementation(project(":platform:lib-java"))
    implementation(project(":platform:restate-defs-java"))
    implementation(libs.spring.boot.starter.web)
    implementation(libs.spring.kafka)
    implementation(libs.restate.sdk.api)
    implementation(libs.restate.sdk.common)
    implementation(libs.restate.sdk.http.vertx)

    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.mockito.core)
    testImplementation(libs.assertj.core)
    testRuntimeOnly(libs.junit.platform.launcher)
}
```

- [ ] **Step 3: Write `services/inventory-service/src/main/resources/application.yml`**

```yaml
server:
  port: 8082

spring:
  application:
    name: inventory-service
  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}

app:
  kafka:
    consumers:
      enabled: ${KAFKA_CONSUMERS_ENABLED:true}
  restate:
    register-handlers: ${RESTATE_REGISTER_HANDLERS:true}
    ingress:
      url: ${RESTATE_INGRESS_URL:http://localhost:9070}
    handler:
      port: ${RESTATE_HANDLER_PORT:9082}
```

- [ ] **Step 4: Write `services/inventory-service/src/test/resources/application-test.yml`**

```yaml
server:
  port: 0

spring:
  kafka:
    bootstrap-servers: localhost:0

app:
  kafka:
    consumers:
      enabled: false
  restate:
    register-handlers: false
    ingress:
      url: http://example.invalid
    handler:
      port: 0
```

- [ ] **Step 5: Write `InventoryApplication`**

`services/inventory-service/src/main/java/com/canary/inventory/InventoryApplication.java`:

```java
package com.canary.inventory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class InventoryApplication {
    public static void main(String[] args) {
        SpringApplication.run(InventoryApplication.class, args);
    }
}
```

- [ ] **Step 6: Write the smoke test**

`services/inventory-service/src/test/java/com/canary/inventory/InventoryApplicationTest.java`:

```java
package com.canary.inventory;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("test")
class InventoryApplicationTest {
    @Test
    void contextLoads() {}
}
```

- [ ] **Step 7: Run the test**

Run: `./gradlew :services:inventory-service:test --quiet`

Expected: BUILD SUCCESSFUL, 1 test passes.

- [ ] **Step 8: Commit**

```bash
git add settings.gradle.kts services/inventory-service/
git commit -m "feat(inventory-service): scaffold Gradle subproject + smoke test"
```

---

### Task 13: ReservationController + ReservationStore + Ingress delegation

**Files:**
- Create: `services/inventory-service/src/main/java/com/canary/inventory/store/ReservationStore.java`
- Create: `services/inventory-service/src/main/java/com/canary/inventory/config/IngressClientConfig.java`
- Create: `services/inventory-service/src/main/java/com/canary/inventory/controller/ReservationController.java`
- Create: `services/inventory-service/src/test/java/com/canary/inventory/controller/ReservationControllerTest.java`
- Create: `services/inventory-service/src/test/java/com/canary/inventory/store/ReservationStoreTest.java`

- [ ] **Step 1: Write `ReservationStore`**

```java
package com.canary.inventory.store;

import com.canary.restate.inventory.Reservation;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class ReservationStore {

    private final List<Reservation> reservations = new CopyOnWriteArrayList<>();

    /** Initial inventory per SKU; thin reference value. */
    private static final int INITIAL_PER_SKU = 100;

    public void put(Reservation reservation) {
        reservations.add(reservation);
    }

    public Optional<Reservation> findById(String id) {
        return reservations.stream()
            .filter(r -> id.equals(r.id()))
            .findFirst();
    }

    public int availableFor(String sku) {
        int reserved = reservations.stream()
            .filter(r -> sku.equals(r.sku()))
            .mapToInt(Reservation::quantity)
            .sum();
        return Math.max(0, INITIAL_PER_SKU - reserved);
    }

    public List<Reservation> all() {
        return new ArrayList<>(reservations);
    }
}
```

- [ ] **Step 2: Write the failing store test**

`services/inventory-service/src/test/java/com/canary/inventory/store/ReservationStoreTest.java`:

```java
package com.canary.inventory.store;

import com.canary.restate.inventory.Reservation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ReservationStoreTest {

    ReservationStore store;

    @BeforeEach
    void setUp() {
        store = new ReservationStore();
    }

    @Test
    void availableForReturns100ForUnreservedSku() {
        assertThat(store.availableFor("widget")).isEqualTo(100);
    }

    @Test
    void availableForSubtractsReservedQuantity() {
        store.put(new Reservation("r_1", "widget", 30, "ord_1", "reserved"));
        store.put(new Reservation("r_2", "widget", 10, "ord_2", "reserved"));

        assertThat(store.availableFor("widget")).isEqualTo(60);
    }

    @Test
    void availableForReturnsZeroWhenOverdrawn() {
        store.put(new Reservation("r_1", "widget", 200, "ord_1", "reserved"));

        assertThat(store.availableFor("widget")).isZero();
    }

    @Test
    void availableForIsolatesSkusFromEachOther() {
        store.put(new Reservation("r_1", "widget", 50, "ord_1", "reserved"));

        assertThat(store.availableFor("widget")).isEqualTo(50);
        assertThat(store.availableFor("gadget")).isEqualTo(100);
    }
}
```

Run: `./gradlew :services:inventory-service:test --tests ReservationStoreTest --quiet`. Expected: 4 tests pass.

- [ ] **Step 3: Write `IngressClientConfig`**

```java
package com.canary.inventory.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestClient;

@Configuration
public class IngressClientConfig {

    @Bean
    public RestClient ingressRestClient(
            RestClient.Builder builder,
            @Value("${app.restate.ingress.url}") String ingressUrl
    ) {
        return builder.baseUrl(ingressUrl).build();
    }
}
```

- [ ] **Step 4: Write `ReservationController`**

```java
package com.canary.inventory.controller;

import com.canary.inventory.store.ReservationStore;
import com.canary.restate.inventory.AvailabilityResponse;
import com.canary.restate.inventory.Reservation;
import com.canary.restate.inventory.ReservationRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestClient;

@RestController
public class ReservationController {

    private final RestClient ingressClient;
    private final ReservationStore store;

    public ReservationController(RestClient ingressClient, ReservationStore store) {
        this.ingressClient = ingressClient;
        this.store = store;
    }

    @PostMapping("/reservations")
    public ResponseEntity<Reservation> create(@RequestBody ReservationRequest req) {
        // Workflow URL: /ReservationWorkflow/{workflowId}/run; key on orderId so retries are idempotent at the workflow level.
        Reservation reservation = ingressClient.post()
            .uri("/ReservationWorkflow/{key}/run", req.orderId())
            .body(req)
            .retrieve()
            .body(Reservation.class);
        return ResponseEntity.status(HttpStatus.CREATED).body(reservation);
    }

    @GetMapping("/products/{sku}/availability")
    public AvailabilityResponse availability(@PathVariable("sku") String sku) {
        return new AvailabilityResponse(sku, store.availableFor(sku));
    }
}
```

- [ ] **Step 5: Write the failing controller test**

`services/inventory-service/src/test/java/com/canary/inventory/controller/ReservationControllerTest.java`:

```java
package com.canary.inventory.controller;

import com.canary.inventory.store.ReservationStore;
import com.canary.restate.inventory.Reservation;
import com.canary.restate.inventory.ReservationRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.client.RestClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(ReservationController.class)
class ReservationControllerTest {

    @Autowired MockMvc mockMvc;
    @Autowired ObjectMapper objectMapper;

    @MockitoBean RestClient ingressClient;
    @MockitoBean ReservationStore store;

    @Test
    void postDelegatesToWorkflowViaIngress() throws Exception {
        var req = new ReservationRequest("widget", 5, "ord_1");
        var returned = new Reservation("r_1", "widget", 5, "ord_1", "reserved");

        var uriSpec = mock(RestClient.RequestBodyUriSpec.class);
        var bodySpec = mock(RestClient.RequestBodySpec.class);
        var responseSpec = mock(RestClient.ResponseSpec.class);
        when(ingressClient.post()).thenReturn(uriSpec);
        when(uriSpec.uri(eq("/ReservationWorkflow/{key}/run"), eq("ord_1"))).thenReturn(bodySpec);
        when(bodySpec.body(any(ReservationRequest.class))).thenReturn(bodySpec);
        when(bodySpec.retrieve()).thenReturn(responseSpec);
        when(responseSpec.body(Reservation.class)).thenReturn(returned);

        mockMvc.perform(post("/reservations")
                .contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(req)))
            .andExpect(status().isCreated())
            .andExpect(jsonPath("$.id").value("r_1"))
            .andExpect(jsonPath("$.status").value("reserved"));

        var captor = ArgumentCaptor.forClass(ReservationRequest.class);
        verify(bodySpec).body(captor.capture());
        assertThat(captor.getValue()).isEqualTo(req);
    }

    @Test
    void availabilityReadsFromStore() throws Exception {
        when(store.availableFor("widget")).thenReturn(73);

        mockMvc.perform(get("/products/widget/availability"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.sku").value("widget"))
            .andExpect(jsonPath("$.available").value(73));
    }
}
```

- [ ] **Step 6: Run all inventory-service tests**

Run: `./gradlew :services:inventory-service:test --quiet`

Expected: 7 tests pass (smoke + 4 store + 2 controller).

- [ ] **Step 7: Commit**

```bash
git add services/inventory-service/
git commit -m "feat(inventory-service): ReservationController delegates to Workflow via Ingress; availability reads store"
```

---

### Task 14: ReservationWorkflowImpl Restate handler + endpoint config (gated)

**Files:**
- Create: `services/inventory-service/src/main/java/com/canary/inventory/handler/ReservationWorkflowImpl.java`
- Create: `services/inventory-service/src/main/java/com/canary/inventory/config/RestateEndpointConfig.java`
- Create: `services/inventory-service/src/test/java/com/canary/inventory/handler/ReservationWorkflowImplTest.java`
- Create: `services/inventory-service/src/test/java/com/canary/inventory/config/RestateEndpointGatingTest.java`

- [ ] **Step 1: Write `ReservationWorkflowImpl`** (Kafka emission added in Task 15)

```java
package com.canary.inventory.handler;

import com.canary.inventory.store.ReservationStore;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import com.canary.restate.audit.AuditEvent;
import com.canary.restate.audit.AuditQueryService;
import com.canary.restate.inventory.Reservation;
import com.canary.restate.inventory.ReservationRequest;
import com.canary.restate.inventory.ReservationWorkflow;
import dev.restate.common.InvocationOptions;
import dev.restate.sdk.WorkflowContext;

import java.util.UUID;

public class ReservationWorkflowImpl extends ReservationWorkflow {

    private final ReservationStore store;
    private final XCanaryRestateClientCustomizer canary;

    public ReservationWorkflowImpl(ReservationStore store, XCanaryRestateClientCustomizer canary) {
        this.store = store;
        this.canary = canary;
    }

    @Override
    public Reservation run(WorkflowContext ctx, ReservationRequest req) {
        // Trimmed-C: just record the reservation. No timer / release-on-expiry — Phase 3.
        Reservation reservation = new Reservation(
            UUID.randomUUID().toString(),
            req.sku(),
            req.quantity(),
            req.orderId(),
            "reserved"
        );
        store.put(reservation);

        InvocationOptions opts = canary.apply(InvocationOptions.builder());
        ctx.serviceClient(AuditQueryService.class)
           .call(AuditQueryService::append,
                 new AuditEvent("inventory", reservation.id(), "reserved", req.orderId()),
                 opts);

        return reservation;
    }
}
```

- [ ] **Step 2: Write the handler unit test**

`services/inventory-service/src/test/java/com/canary/inventory/handler/ReservationWorkflowImplTest.java`:

```java
package com.canary.inventory.handler;

import com.canary.inventory.store.ReservationStore;
import com.canary.platform.lib.XCanaryConstants;
import com.canary.platform.lib.XCanaryContext;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import com.canary.restate.audit.AuditEvent;
import com.canary.restate.audit.AuditQueryService;
import com.canary.restate.inventory.Reservation;
import com.canary.restate.inventory.ReservationRequest;
import dev.restate.common.InvocationOptions;
import dev.restate.sdk.WorkflowContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReservationWorkflowImplTest {

    ReservationStore store;
    XCanaryRestateClientCustomizer canary;
    ReservationWorkflowImpl handler;
    WorkflowContext ctx;
    AuditQueryServiceClientStub auditClient;

    @BeforeEach
    void setUp() {
        store = new ReservationStore();
        canary = new XCanaryRestateClientCustomizer();
        handler = new ReservationWorkflowImpl(store, canary);
        ctx = mock(WorkflowContext.class);
        auditClient = mock(AuditQueryServiceClientStub.class);
        when(ctx.serviceClient(eq(AuditQueryService.class))).thenAnswer(inv -> auditClient);
    }

    @AfterEach
    void clearCanary() {
        XCanaryContext.clear();
    }

    @Test
    void runRecordsReservationAndCallsAudit() {
        Reservation result = handler.run(ctx, new ReservationRequest("widget", 5, "ord_1"));

        assertThat(result.sku()).isEqualTo("widget");
        assertThat(result.quantity()).isEqualTo(5);
        assertThat(result.orderId()).isEqualTo("ord_1");
        assertThat(result.status()).isEqualTo("reserved");
        assertThat(store.findById(result.id())).contains(result);

        var auditCap = ArgumentCaptor.forClass(AuditEvent.class);
        verify(auditClient).call(any(), auditCap.capture(), any(InvocationOptions.class));
        AuditEvent event = auditCap.getValue();
        assertThat(event.aggregate()).isEqualTo("inventory");
        assertThat(event.id()).isEqualTo(result.id());
        assertThat(event.action()).isEqualTo("reserved");
        assertThat(event.correlationId()).isEqualTo("ord_1");
    }

    @Test
    void runStampsXCanaryOnAuditCallWhenContextIsCanary() {
        XCanaryContext.set(true);

        handler.run(ctx, new ReservationRequest("widget", 1, "ord_1"));

        var optsCap = ArgumentCaptor.forClass(InvocationOptions.class);
        verify(auditClient).call(any(), any(AuditEvent.class), optsCap.capture());
        assertThat(optsCap.getValue().getHeaders())
            .containsEntry(XCanaryConstants.HEADER_NAME, XCanaryConstants.TRUE_VALUE);
    }

    interface AuditQueryServiceClientStub {
        void call(java.util.function.BiConsumer<AuditQueryService, AuditEvent> ref,
                  AuditEvent input,
                  InvocationOptions opts);
    }
}
```

- [ ] **Step 3: Write `RestateEndpointConfig`**

```java
package com.canary.inventory.config;

import com.canary.inventory.handler.ReservationWorkflowImpl;
import com.canary.inventory.store.ReservationStore;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import dev.restate.sdk.endpoint.Endpoint;
import dev.restate.sdk.http.vertx.RestateHttpServer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnProperty(
    name = "app.restate.register-handlers",
    havingValue = "true",
    matchIfMissing = true
)
public class RestateEndpointConfig {

    private final int port;
    private final ReservationWorkflowImpl handler;
    private RestateHttpServer server;

    public RestateEndpointConfig(@Value("${app.restate.handler.port}") int port,
                                 ReservationWorkflowImpl handler) {
        this.port = port;
        this.handler = handler;
    }

    @Bean
    public static ReservationWorkflowImpl reservationWorkflowImpl(
            ReservationStore store,
            XCanaryRestateClientCustomizer canary
    ) {
        return new ReservationWorkflowImpl(store, canary);
    }

    @PostConstruct
    void start() {
        server = RestateHttpServer.fromEndpoint(
            Endpoint.builder().bind(handler).build()
        );
        server.listen(port);
    }

    @PreDestroy
    void stop() throws Exception {
        if (server != null) {
            server.close();
        }
    }
}
```

- [ ] **Step 4: Write the gating test**

`services/inventory-service/src/test/java/com/canary/inventory/config/RestateEndpointGatingTest.java`:

```java
package com.canary.inventory.config;

import com.canary.inventory.store.ReservationStore;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

class RestateEndpointGatingTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withUserConfiguration(TestStubs.class, RestateEndpointConfig.class)
        .withPropertyValues("app.restate.handler.port=0");

    @Test
    void whenFlagTrueThenEndpointConfigIsActive() {
        runner.withPropertyValues("app.restate.register-handlers=true")
            .run(ctx -> assertThat(ctx).hasSingleBean(RestateEndpointConfig.class));
    }

    @Test
    void whenFlagFalseThenEndpointConfigIsSkipped() {
        runner.withPropertyValues("app.restate.register-handlers=false")
            .run(ctx -> assertThat(ctx).doesNotHaveBean(RestateEndpointConfig.class));
    }

    @Test
    void whenFlagAbsentThenDefaultsToActive() {
        runner.run(ctx -> assertThat(ctx).hasSingleBean(RestateEndpointConfig.class));
    }

    @Configuration
    static class TestStubs {
        @Bean ReservationStore reservationStore() { return new ReservationStore(); }
        @Bean XCanaryRestateClientCustomizer canary() { return new XCanaryRestateClientCustomizer(); }
    }
}
```

- [ ] **Step 5: Run all inventory-service tests**

Run: `./gradlew :services:inventory-service:test --quiet`

Expected: ~12 tests pass.

- [ ] **Step 6: Commit**

```bash
git add services/inventory-service/
git commit -m "feat(inventory-service): ReservationWorkflow Restate handler + audit R-to-R + gated endpoint"
```

---

### Task 15: Kafka producer (`inventory.events`) + consumer (`orders.events`, gated) + `/internal/consumed-events`

**Files:**
- Create: `services/inventory-service/src/main/java/com/canary/inventory/kafka/KafkaProducerConfig.java`
- Create: `services/inventory-service/src/main/java/com/canary/inventory/kafka/InventoryKafkaListener.java`
- Create: `services/inventory-service/src/main/java/com/canary/inventory/store/ConsumedEvent.java`
- Create: `services/inventory-service/src/main/java/com/canary/inventory/store/ConsumedEventStore.java`
- Create: `services/inventory-service/src/main/java/com/canary/inventory/controller/InternalController.java`
- Modify: `services/inventory-service/src/main/java/com/canary/inventory/handler/ReservationWorkflowImpl.java` (Kafka emission)
- Modify: `services/inventory-service/src/main/java/com/canary/inventory/config/RestateEndpointConfig.java` (KafkaTemplate + ObjectMapper deps)
- Modify: `services/inventory-service/src/test/java/com/canary/inventory/handler/ReservationWorkflowImplTest.java`
- Create: `services/inventory-service/src/test/java/com/canary/inventory/kafka/KafkaProducerConfigTest.java`
- Create: `services/inventory-service/src/test/java/com/canary/inventory/kafka/InventoryKafkaListenerGatingTest.java`
- Create: `services/inventory-service/src/test/java/com/canary/inventory/controller/InternalControllerTest.java`

- [ ] **Step 1: Write `ConsumedEvent` + `ConsumedEventStore`**

`services/inventory-service/src/main/java/com/canary/inventory/store/ConsumedEvent.java`:

```java
package com.canary.inventory.store;

import java.util.Map;

public record ConsumedEvent(String topic, String key, String value, Map<String, String> headers) {}
```

`services/inventory-service/src/main/java/com/canary/inventory/store/ConsumedEventStore.java`:

```java
package com.canary.inventory.store;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Component
public class ConsumedEventStore {

    private final List<ConsumedEvent> events = new CopyOnWriteArrayList<>();

    public void record(ConsumedEvent event) {
        events.add(event);
    }

    public List<ConsumedEvent> all() {
        return new ArrayList<>(events);
    }
}
```

- [ ] **Step 2: Write `InternalController`**

```java
package com.canary.inventory.controller;

import com.canary.inventory.store.ConsumedEvent;
import com.canary.inventory.store.ConsumedEventStore;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class InternalController {

    private final ConsumedEventStore store;

    public InternalController(ConsumedEventStore store) {
        this.store = store;
    }

    @GetMapping("/internal/consumed-events")
    public List<ConsumedEvent> consumedEvents() {
        return store.all();
    }
}
```

- [ ] **Step 3: Write the `InternalController` test**

`services/inventory-service/src/test/java/com/canary/inventory/controller/InternalControllerTest.java`:

```java
package com.canary.inventory.controller;

import com.canary.inventory.store.ConsumedEvent;
import com.canary.inventory.store.ConsumedEventStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(InternalController.class)
class InternalControllerTest {

    @Autowired MockMvc mockMvc;
    @MockitoBean ConsumedEventStore store;

    @Test
    void consumedEventsEndpointReturnsRecordedEvents() throws Exception {
        when(store.all()).thenReturn(List.of(
            new ConsumedEvent("orders.events", "ord_1", "{}", Map.of("x-canary", "true"))
        ));

        mockMvc.perform(get("/internal/consumed-events"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$[0].topic").value("orders.events"))
            .andExpect(jsonPath("$[0].headers['x-canary']").value("true"));
    }
}
```

- [ ] **Step 4: Write `KafkaProducerConfig`**

```java
package com.canary.inventory.kafka;

import com.canary.platform.lib.XCanaryKafkaProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Bean
    public ProducerFactory<String, String> producerFactory(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers
    ) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, XCanaryKafkaProducerInterceptor.class.getName());
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> pf) {
        return new KafkaTemplate<>(pf);
    }
}
```

- [ ] **Step 5: Write the producer config test**

`services/inventory-service/src/test/java/com/canary/inventory/kafka/KafkaProducerConfigTest.java`:

```java
package com.canary.inventory.kafka;

import com.canary.platform.lib.XCanaryKafkaProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaProducerConfigTest {

    @Test
    void producerFactoryHasXCanaryInterceptorConfigured() {
        var config = new KafkaProducerConfig();
        ProducerFactory<String, String> factory = config.producerFactory("localhost:9092");

        var props = ((DefaultKafkaProducerFactory<String, String>) factory).getConfigurationProperties();

        assertThat(props.get(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG))
            .isEqualTo(XCanaryKafkaProducerInterceptor.class.getName());
    }
}
```

- [ ] **Step 6: Update `ReservationWorkflowImpl` to emit `inventory.events`**

```java
package com.canary.inventory.handler;

import com.canary.inventory.store.ReservationStore;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import com.canary.restate.audit.AuditEvent;
import com.canary.restate.audit.AuditQueryService;
import com.canary.restate.inventory.Reservation;
import com.canary.restate.inventory.ReservationRequest;
import com.canary.restate.inventory.ReservationWorkflow;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.restate.common.InvocationOptions;
import dev.restate.sdk.WorkflowContext;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.UUID;

public class ReservationWorkflowImpl extends ReservationWorkflow {

    private final ReservationStore store;
    private final XCanaryRestateClientCustomizer canary;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public ReservationWorkflowImpl(ReservationStore store,
                                   XCanaryRestateClientCustomizer canary,
                                   KafkaTemplate<String, String> kafkaTemplate,
                                   ObjectMapper objectMapper) {
        this.store = store;
        this.canary = canary;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    public Reservation run(WorkflowContext ctx, ReservationRequest req) {
        Reservation reservation = new Reservation(
            UUID.randomUUID().toString(),
            req.sku(),
            req.quantity(),
            req.orderId(),
            "reserved"
        );
        store.put(reservation);

        try {
            kafkaTemplate.send("inventory.events", reservation.id(),
                               objectMapper.writeValueAsString(reservation));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize Reservation", e);
        }

        InvocationOptions opts = canary.apply(InvocationOptions.builder());
        ctx.serviceClient(AuditQueryService.class)
           .call(AuditQueryService::append,
                 new AuditEvent("inventory", reservation.id(), "reserved", req.orderId()),
                 opts);

        return reservation;
    }
}
```

- [ ] **Step 7: Update `RestateEndpointConfig`'s static bean to wire new deps**

In `services/inventory-service/src/main/java/com/canary/inventory/config/RestateEndpointConfig.java`:

```java
@Bean
public static ReservationWorkflowImpl reservationWorkflowImpl(
        ReservationStore store,
        XCanaryRestateClientCustomizer canary,
        KafkaTemplate<String, String> kafkaTemplate,
        com.fasterxml.jackson.databind.ObjectMapper objectMapper
) {
    return new ReservationWorkflowImpl(store, canary, kafkaTemplate, objectMapper);
}
```

Add the import: `import org.springframework.kafka.core.KafkaTemplate;`.

Also update `services/inventory-service/src/test/java/com/canary/inventory/config/RestateEndpointGatingTest.java` — replace its `TestStubs`:

```java
@Configuration
static class TestStubs {
    @Bean ReservationStore reservationStore() { return new ReservationStore(); }
    @Bean XCanaryRestateClientCustomizer canary() { return new XCanaryRestateClientCustomizer(); }
    @Bean
    @SuppressWarnings("unchecked")
    KafkaTemplate<String, String> kafkaTemplate() {
        return org.mockito.Mockito.mock(KafkaTemplate.class);
    }
    @Bean com.fasterxml.jackson.databind.ObjectMapper objectMapper() {
        return new com.fasterxml.jackson.databind.ObjectMapper();
    }
}
```

Add the import: `import org.springframework.kafka.core.KafkaTemplate;`.

- [ ] **Step 8: Update `ReservationWorkflowImplTest` constructor + add Kafka assertion**

Update `setUp()`:

```java
KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
ObjectMapper objectMapper = new ObjectMapper();
// ...
handler = new ReservationWorkflowImpl(store, canary, kafkaTemplate, objectMapper);
```

Add the test:

```java
@Test
void runEmitsInventoryEvent() throws Exception {
    Reservation result = handler.run(ctx, new ReservationRequest("widget", 3, "ord_1"));

    var keyCap = ArgumentCaptor.forClass(String.class);
    var valueCap = ArgumentCaptor.forClass(String.class);
    verify(kafkaTemplate).send(eq("inventory.events"), keyCap.capture(), valueCap.capture());
    assertThat(keyCap.getValue()).isEqualTo(result.id());
    assertThat(objectMapper.readValue(valueCap.getValue(), Reservation.class)).isEqualTo(result);
}
```

Imports added: `com.fasterxml.jackson.databind.ObjectMapper`, `org.springframework.kafka.core.KafkaTemplate`.

- [ ] **Step 9: Write `InventoryKafkaListener`** (consumes `orders.events`)

```java
package com.canary.inventory.kafka;

import com.canary.inventory.store.ConsumedEvent;
import com.canary.inventory.store.ConsumedEventStore;
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

    public InventoryKafkaListener(ConsumedEventStore store) {
        this.store = store;
    }

    @KafkaListener(topics = "orders.events", groupId = "inventory-service")
    public void onMessage(ConsumerRecord<String, String> record) {
        Map<String, String> headers = new HashMap<>();
        record.headers().forEach(h -> headers.put(h.key(), new String(h.value(), StandardCharsets.UTF_8)));
        store.record(new ConsumedEvent(record.topic(), record.key(), record.value(), headers));
    }
}
```

- [ ] **Step 10: Write the consumer gating test**

`services/inventory-service/src/test/java/com/canary/inventory/kafka/InventoryKafkaListenerGatingTest.java`:

```java
package com.canary.inventory.kafka;

import com.canary.inventory.store.ConsumedEventStore;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

class InventoryKafkaListenerGatingTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class))
        .withUserConfiguration(TestStubs.class, InventoryKafkaListener.class)
        .withPropertyValues("spring.kafka.bootstrap-servers=localhost:0");

    @Test
    void whenFlagTrueThenListenerIsRegistered() {
        runner.withPropertyValues("app.kafka.consumers.enabled=true")
            .run(ctx -> assertThat(ctx).hasSingleBean(InventoryKafkaListener.class));
    }

    @Test
    void whenFlagFalseThenListenerIsAbsent() {
        runner.withPropertyValues("app.kafka.consumers.enabled=false")
            .run(ctx -> assertThat(ctx).doesNotHaveBean(InventoryKafkaListener.class));
    }

    @Test
    void whenFlagAbsentThenDefaultsToActive() {
        runner.run(ctx -> assertThat(ctx).hasSingleBean(InventoryKafkaListener.class));
    }

    @Configuration
    static class TestStubs {
        @Bean ConsumedEventStore consumedEventStore() { return new ConsumedEventStore(); }
    }
}
```

- [ ] **Step 11: Run all inventory-service tests**

Run: `./gradlew :services:inventory-service:test --quiet`

Expected: ~16 tests pass.

- [ ] **Step 12: Commit**

```bash
git add services/inventory-service/
git commit -m "feat(inventory-service): Kafka producer/consumer (gated) + inventory.events emission + /internal/consumed-events"
```

---

## Phase E — notification-service (Node + Express + KafkaJS, `@Service`)

First Node service. Pattern: thin `index.ts` boots three setup functions (`setupHttp`, `setupKafka`, `setupRestate`) — each accepts deps as args so unit tests can drive them without `app.listen()` etc. ALS context for Restate handlers is established via lib-node's `runWithCanary(boolean, fn)` so `applyXCanaryToRestateOptions` works inside the handler.

### Task 16: Scaffold `services/notification-service` pnpm package

**Files:**
- Create: `services/notification-service/package.json`
- Create: `services/notification-service/tsconfig.json`
- Create: `services/notification-service/vitest.config.ts`
- Create: `services/notification-service/.env.example`
- Create: `services/notification-service/src/config.ts`
- Create: `services/notification-service/src/store.ts`
- Create: `services/notification-service/src/index.ts`
- Create: `services/notification-service/src/__tests__/config.test.ts`

- [ ] **Step 1: Write `services/notification-service/package.json`**

```json
{
  "name": "@canary/notification-service",
  "version": "0.0.1",
  "private": true,
  "type": "module",
  "main": "./dist/index.js",
  "scripts": {
    "build": "tsc -p tsconfig.json",
    "test": "vitest run",
    "test:watch": "vitest",
    "start": "node dist/index.js"
  },
  "dependencies": {
    "@canary/lib-node": "workspace:*",
    "@canary/restate-defs-node": "workspace:*",
    "@restatedev/restate-sdk": "^1.14.2",
    "axios": "^1.7.7",
    "express": "^4.21.0",
    "kafkajs": "^2.2.4"
  },
  "devDependencies": {
    "@types/express": "^5.0.0",
    "@types/node": "^22.7.4",
    "@types/supertest": "^6.0.2",
    "supertest": "^7.0.0",
    "typescript": "^5.6.2",
    "vitest": "^2.1.1"
  }
}
```

- [ ] **Step 2: Write `services/notification-service/tsconfig.json`**

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ESNext",
    "moduleResolution": "Bundler",
    "lib": ["ES2022"],
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "outDir": "./dist",
    "rootDir": "./src",
    "declaration": false,
    "resolveJsonModule": true,
    "isolatedModules": true
  },
  "include": ["src/**/*.ts"],
  "exclude": ["src/**/__tests__/**", "dist", "node_modules"]
}
```

- [ ] **Step 3: Write `services/notification-service/vitest.config.ts`**

```typescript
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["src/**/__tests__/**/*.test.ts"],
    environment: "node",
  },
});
```

- [ ] **Step 4: Write `services/notification-service/.env.example`**

```
PORT=3002
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_CONSUMERS_ENABLED=true
RESTATE_INGRESS_URL=http://localhost:9070
RESTATE_REGISTER_HANDLERS=true
RESTATE_HANDLER_PORT=9085
```

- [ ] **Step 5: Write `services/notification-service/src/config.ts`**

```typescript
export interface AppConfig {
  HTTP_PORT: number;
  KAFKA_BOOTSTRAP_SERVERS: string[];
  KAFKA_CONSUMERS_ENABLED: boolean;
  RESTATE_INGRESS_URL: string;
  RESTATE_REGISTER_HANDLERS: boolean;
  RESTATE_HANDLER_PORT: number;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  return {
    HTTP_PORT: Number(env.PORT ?? 3002),
    KAFKA_BOOTSTRAP_SERVERS: (env.KAFKA_BOOTSTRAP_SERVERS ?? "localhost:9092").split(","),
    KAFKA_CONSUMERS_ENABLED: env.KAFKA_CONSUMERS_ENABLED !== "false",
    RESTATE_INGRESS_URL: env.RESTATE_INGRESS_URL ?? "http://localhost:9070",
    RESTATE_REGISTER_HANDLERS: env.RESTATE_REGISTER_HANDLERS !== "false",
    RESTATE_HANDLER_PORT: Number(env.RESTATE_HANDLER_PORT ?? 9085),
  };
}
```

- [ ] **Step 6: Write the failing `config` test**

`services/notification-service/src/__tests__/config.test.ts`:

```typescript
import { describe, it, expect } from "vitest";
import { loadConfig } from "../config.js";

describe("loadConfig", () => {
  it("uses defaults when env is empty", () => {
    const cfg = loadConfig({});
    expect(cfg.HTTP_PORT).toBe(3002);
    expect(cfg.KAFKA_BOOTSTRAP_SERVERS).toEqual(["localhost:9092"]);
    expect(cfg.KAFKA_CONSUMERS_ENABLED).toBe(true);
    expect(cfg.RESTATE_REGISTER_HANDLERS).toBe(true);
    expect(cfg.RESTATE_INGRESS_URL).toBe("http://localhost:9070");
    expect(cfg.RESTATE_HANDLER_PORT).toBe(9085);
  });

  it("treats KAFKA_CONSUMERS_ENABLED=false as false", () => {
    const cfg = loadConfig({ KAFKA_CONSUMERS_ENABLED: "false" });
    expect(cfg.KAFKA_CONSUMERS_ENABLED).toBe(false);
  });

  it("treats RESTATE_REGISTER_HANDLERS=false as false", () => {
    const cfg = loadConfig({ RESTATE_REGISTER_HANDLERS: "false" });
    expect(cfg.RESTATE_REGISTER_HANDLERS).toBe(false);
  });

  it("treats any other value as true (unset → enabled)", () => {
    expect(loadConfig({ KAFKA_CONSUMERS_ENABLED: "anything" }).KAFKA_CONSUMERS_ENABLED).toBe(true);
    expect(loadConfig({}).KAFKA_CONSUMERS_ENABLED).toBe(true);
  });

  it("splits KAFKA_BOOTSTRAP_SERVERS on commas", () => {
    const cfg = loadConfig({ KAFKA_BOOTSTRAP_SERVERS: "a:9092,b:9092,c:9092" });
    expect(cfg.KAFKA_BOOTSTRAP_SERVERS).toEqual(["a:9092", "b:9092", "c:9092"]);
  });
});
```

- [ ] **Step 7: Write `services/notification-service/src/store.ts`**

```typescript
import type { Notification } from "@canary/restate-defs-node";

export interface ConsumedEvent {
  topic: string;
  key: string | null;
  value: string;
  headers: Record<string, string>;
}

class NotificationStore {
  private byId = new Map<string, Notification>();

  put(n: Notification): void {
    this.byId.set(n.id, n);
  }

  byUserId(userId: string): Notification[] {
    return Array.from(this.byId.values()).filter((n) => n.userId === userId);
  }

  all(): Notification[] {
    return Array.from(this.byId.values());
  }
}

class ConsumedEventStore {
  private events: ConsumedEvent[] = [];

  record(e: ConsumedEvent): void {
    this.events.push(e);
  }

  all(): ConsumedEvent[] {
    return [...this.events];
  }
}

export const notificationStore = new NotificationStore();
export const consumedEventStore = new ConsumedEventStore();
```

- [ ] **Step 8: Write `services/notification-service/src/index.ts` (skeleton — populated in next tasks)**

```typescript
import { loadConfig } from "./config.js";

const config = loadConfig();

console.log("notification-service booting", {
  httpPort: config.HTTP_PORT,
  restateRegisterHandlers: config.RESTATE_REGISTER_HANDLERS,
  kafkaConsumersEnabled: config.KAFKA_CONSUMERS_ENABLED,
});

// HTTP, Kafka, Restate setup are wired in Tasks 17, 18, 19.
```

- [ ] **Step 9: Run install + tests**

Run: `pnpm install`
Expected: links the new workspace package; `@canary/lib-node` and `@canary/restate-defs-node` resolve.

Run: `pnpm --filter @canary/notification-service test`
Expected: 5 tests pass (the config tests).

- [ ] **Step 10: Commit**

```bash
git add pnpm-lock.yaml services/notification-service/
git commit -m "feat(notification-service): scaffold pnpm package + config + store skeleton"
```

---

### Task 17: HTTP routes + Ingress delegation

**Files:**
- Create: `services/notification-service/src/http.ts`
- Modify: `services/notification-service/src/index.ts` (wire setupHttp)
- Create: `services/notification-service/src/__tests__/http.test.ts`

- [ ] **Step 1: Write `services/notification-service/src/http.ts`**

```typescript
import express, { type Express } from "express";
import axios, { type AxiosInstance } from "axios";
import { xCanaryMiddleware, attachXCanaryAxiosInterceptor } from "@canary/lib-node";
import type { NotifyRequest } from "@canary/restate-defs-node";
import { notificationStore } from "./store.js";

export interface HttpDeps {
  ingressClient: AxiosInstance;
}

export function buildIngressClient(ingressUrl: string): AxiosInstance {
  const client = axios.create({ baseURL: ingressUrl });
  attachXCanaryAxiosInterceptor(client);
  return client;
}

export function setupHttp(deps: HttpDeps): Express {
  const app = express();
  app.use(express.json());
  app.use(xCanaryMiddleware());

  app.post("/notifications", async (req, res) => {
    const body = req.body as NotifyRequest;
    try {
      const response = await deps.ingressClient.post(
        "/NotificationService/notify",
        body,
      );
      res.status(201).json(response.data);
    } catch (err) {
      console.error("ingress call failed", err);
      res.status(502).json({ error: "ingress_failed" });
    }
  });

  app.get("/notifications/by-user/:userId", (req, res) => {
    const found = notificationStore.byUserId(req.params.userId);
    res.json(found);
  });

  app.get("/internal/consumed-events", (_req, res) => {
    const { consumedEventStore } = require("./store.js");
    res.json(consumedEventStore.all());
  });

  return app;
}
```

- [ ] **Step 2: Update `services/notification-service/src/index.ts` to use setupHttp**

```typescript
import { loadConfig } from "./config.js";
import { setupHttp, buildIngressClient } from "./http.js";

const config = loadConfig();

const ingressClient = buildIngressClient(config.RESTATE_INGRESS_URL);
const app = setupHttp({ ingressClient });

app.listen(config.HTTP_PORT, () => {
  console.log(`notification-service HTTP listening on ${config.HTTP_PORT}`);
});

// Kafka + Restate setup wired in Tasks 18 + 19.
```

- [ ] **Step 3: Write the failing HTTP test**

`services/notification-service/src/__tests__/http.test.ts`:

```typescript
import { describe, it, expect, beforeEach, vi } from "vitest";
import request from "supertest";
import type { AxiosInstance } from "axios";
import { setupHttp } from "../http.js";
import { notificationStore } from "../store.js";

function mockAxios(): AxiosInstance {
  return {
    post: vi.fn(),
    get: vi.fn(),
  } as unknown as AxiosInstance;
}

describe("HTTP routes", () => {
  beforeEach(() => {
    // Reset store between tests
    (notificationStore as unknown as { byId: Map<string, unknown> }).byId.clear();
  });

  it("POST /notifications delegates to Restate Ingress", async () => {
    const ingressClient = mockAxios();
    const returned = { id: "n_1", userId: "u_1", message: "hi", status: "sent" };
    (ingressClient.post as ReturnType<typeof vi.fn>).mockResolvedValue({ data: returned });

    const app = setupHttp({ ingressClient });

    const body = { userId: "u_1", message: "hi", orderId: "ord_1" };
    const res = await request(app)
      .post("/notifications")
      .set("content-type", "application/json")
      .send(body);

    expect(res.status).toBe(201);
    expect(res.body).toEqual(returned);
    expect(ingressClient.post).toHaveBeenCalledWith("/NotificationService/notify", body);
  });

  it("POST /notifications returns 502 when Ingress call fails", async () => {
    const ingressClient = mockAxios();
    (ingressClient.post as ReturnType<typeof vi.fn>).mockRejectedValue(new Error("network"));

    const app = setupHttp({ ingressClient });

    const res = await request(app)
      .post("/notifications")
      .send({ userId: "u_1", message: "hi", orderId: "ord_1" });

    expect(res.status).toBe(502);
    expect(res.body).toEqual({ error: "ingress_failed" });
  });

  it("GET /notifications/by-user/:userId reads store directly", async () => {
    notificationStore.put({ id: "n_1", userId: "u_42", message: "hi", status: "sent" });
    notificationStore.put({ id: "n_2", userId: "u_99", message: "ho", status: "sent" });

    const app = setupHttp({ ingressClient: mockAxios() });

    const res = await request(app).get("/notifications/by-user/u_42");

    expect(res.status).toBe(200);
    expect(res.body).toEqual([{ id: "n_1", userId: "u_42", message: "hi", status: "sent" }]);
  });
});
```

- [ ] **Step 4: Run the test**

Run: `pnpm --filter @canary/notification-service test`

Expected: 5 config tests + 3 HTTP tests = 8 tests pass.

- [ ] **Step 5: Commit**

```bash
git add services/notification-service/
git commit -m "feat(notification-service): HTTP routes + Ingress delegation via attachXCanaryAxiosInterceptor"
```

---

### Task 18: NotificationService Restate handler + endpoint setup (gated)

**Files:**
- Create: `services/notification-service/src/restate.ts`
- Modify: `services/notification-service/src/index.ts`
- Create: `services/notification-service/src/__tests__/restate.test.ts`

- [ ] **Step 1: Write `services/notification-service/src/restate.ts`**

```typescript
import * as restate from "@restatedev/restate-sdk";
import { runWithCanary, applyXCanaryToRestateOptions } from "@canary/lib-node";
import {
  auditQueryServiceDef,
  notificationServiceDef,
  type Notification,
  type NotifyRequest,
  type AuditEvent,
} from "@canary/restate-defs-node";
import { notificationStore } from "./store.js";
import { randomUUID } from "node:crypto";

export interface RestateSetupOptions {
  registerHandlers: boolean;
  port: number;
}

export const notificationService = restate.service({
  name: notificationServiceDef.name,
  handlers: {
    notify: async (ctx: restate.Context, req: NotifyRequest): Promise<Notification> => {
      // Read x-canary from invocation metadata (Restate carries HTTP headers as metadata).
      // Adjust the read path to your SDK's API; common shape: ctx.request().headers
      const incoming = (ctx as unknown as { request?: () => { headers?: Record<string, string> } }).request?.();
      const isCanary = incoming?.headers?.["x-canary"] === "true";

      return runWithCanary(isCanary, async () => {
        const notification: Notification = {
          id: randomUUID(),
          userId: req.userId,
          message: req.message,
          status: "sent",
        };
        notificationStore.put(notification);

        const auditEvent: AuditEvent = {
          aggregate: "notification",
          id: notification.id,
          action: "sent",
          correlationId: req.orderId,
        };
        await ctx.serviceClient(auditQueryServiceDef).append(
          auditEvent,
          applyXCanaryToRestateOptions({}),
        );

        return notification;
      });
    },
  },
});

export async function setupRestate(opts: RestateSetupOptions): Promise<void> {
  if (!opts.registerHandlers) {
    console.log("RESTATE_REGISTER_HANDLERS=false; skipping Restate endpoint listener");
    return;
  }
  await restate.endpoint().bind(notificationService).listen(opts.port);
  console.log(`notification-service Restate handlers listening on ${opts.port}`);
}
```

**SDK API note:** the exact way to read incoming metadata in the handler depends on the Restate Node SDK 1.14 API — the `(ctx as { request?: ... })` cast is a defensive shim. If your SDK exposes `ctx.request().headers()` (function call) or `ctx.headers()` directly, adjust accordingly. Preserve the `runWithCanary(isCanary, fn)` wrapping so `applyXCanaryToRestateOptions` works inside the handler.

- [ ] **Step 2: Update `services/notification-service/src/index.ts` to wire Restate**

```typescript
import { loadConfig } from "./config.js";
import { setupHttp, buildIngressClient } from "./http.js";
import { setupRestate } from "./restate.js";

const config = loadConfig();

const ingressClient = buildIngressClient(config.RESTATE_INGRESS_URL);
const app = setupHttp({ ingressClient });

app.listen(config.HTTP_PORT, () => {
  console.log(`notification-service HTTP listening on ${config.HTTP_PORT}`);
});

await setupRestate({
  registerHandlers: config.RESTATE_REGISTER_HANDLERS,
  port: config.RESTATE_HANDLER_PORT,
});

// Kafka setup wired in Task 19.
```

- [ ] **Step 3: Write the failing Restate handler test**

`services/notification-service/src/__tests__/restate.test.ts`:

```typescript
import { describe, it, expect, vi, beforeEach } from "vitest";
import { setupRestate, notificationService } from "../restate.js";
import { notificationStore } from "../store.js";

vi.mock("@restatedev/restate-sdk", async () => {
  const actual = await vi.importActual<typeof import("@restatedev/restate-sdk")>(
    "@restatedev/restate-sdk",
  );
  return {
    ...actual,
    endpoint: vi.fn(() => ({
      bind: vi.fn().mockReturnThis(),
      listen: vi.fn().mockResolvedValue(undefined),
    })),
  };
});

describe("setupRestate gating", () => {
  it("does NOT call endpoint().listen when registerHandlers=false", async () => {
    const restate = await import("@restatedev/restate-sdk");
    (restate.endpoint as ReturnType<typeof vi.fn>).mockClear();

    await setupRestate({ registerHandlers: false, port: 9085 });

    expect(restate.endpoint).not.toHaveBeenCalled();
  });

  it("calls endpoint().bind(svc).listen(port) when registerHandlers=true", async () => {
    const restate = await import("@restatedev/restate-sdk");
    const bindMock = vi.fn().mockReturnThis();
    const listenMock = vi.fn().mockResolvedValue(undefined);
    (restate.endpoint as ReturnType<typeof vi.fn>).mockReturnValue({
      bind: bindMock,
      listen: listenMock,
    });

    await setupRestate({ registerHandlers: true, port: 9085 });

    expect(restate.endpoint).toHaveBeenCalledOnce();
    expect(bindMock).toHaveBeenCalledWith(notificationService);
    expect(listenMock).toHaveBeenCalledWith(9085);
  });
});

describe("NotificationService.notify handler", () => {
  beforeEach(() => {
    (notificationStore as unknown as { byId: Map<string, unknown> }).byId.clear();
  });

  it("writes Notification to store and calls AuditQueryService.append", async () => {
    const auditAppend = vi.fn().mockResolvedValue(undefined);
    const ctx = {
      request: () => ({ headers: {} }),
      serviceClient: vi.fn().mockReturnValue({ append: auditAppend }),
    };

    // Direct invocation of the handler function (typed via the service definition)
    const handlers = (notificationService as unknown as {
      handlers: { notify: (ctx: unknown, req: unknown) => Promise<unknown> };
    }).handlers;

    const result = (await handlers.notify(
      ctx,
      { userId: "u_1", message: "hi", orderId: "ord_1" },
    )) as { id: string; status: string };

    expect(result.status).toBe("sent");
    expect(notificationStore.byUserId("u_1")).toHaveLength(1);

    expect(auditAppend).toHaveBeenCalledOnce();
    const [event, opts] = auditAppend.mock.calls[0];
    expect(event).toMatchObject({ aggregate: "notification", action: "sent", correlationId: "ord_1" });
    // Without x-canary in request headers, opts should NOT have the header.
    expect(opts.headers?.["x-canary"]).toBeUndefined();
  });

  it("stamps x-canary on audit call when incoming request had it", async () => {
    const auditAppend = vi.fn().mockResolvedValue(undefined);
    const ctx = {
      request: () => ({ headers: { "x-canary": "true" } }),
      serviceClient: vi.fn().mockReturnValue({ append: auditAppend }),
    };

    const handlers = (notificationService as unknown as {
      handlers: { notify: (ctx: unknown, req: unknown) => Promise<unknown> };
    }).handlers;

    await handlers.notify(ctx, { userId: "u_1", message: "hi", orderId: "ord_1" });

    const [, opts] = auditAppend.mock.calls[0];
    expect(opts.headers?.["x-canary"]).toBe("true");
  });
});
```

- [ ] **Step 4: Run the test**

Run: `pnpm --filter @canary/notification-service test`

Expected: 8 + 4 = 12 tests pass (5 config + 3 HTTP + 2 setupRestate gating + 2 handler).

- [ ] **Step 5: Commit**

```bash
git add services/notification-service/
git commit -m "feat(notification-service): NotificationService Restate handler + endpoint setup (gated)"
```

---

### Task 19: Kafka producer + consumer (gated) + `/internal/consumed-events`

**Files:**
- Create: `services/notification-service/src/kafka.ts`
- Modify: `services/notification-service/src/index.ts`
- Modify: `services/notification-service/src/restate.ts` (emit `notifications.events` from handler)
- Modify: `services/notification-service/src/__tests__/restate.test.ts` (Kafka assertion)
- Create: `services/notification-service/src/__tests__/kafka.test.ts`

- [ ] **Step 1: Write `services/notification-service/src/kafka.ts`**

```typescript
import { Kafka, Producer, Consumer, type EachMessagePayload } from "kafkajs";
import { stampXCanaryOnProducerRecord } from "@canary/lib-node";
import { consumedEventStore } from "./store.js";

export interface KafkaSetupOptions {
  brokers: string[];
  consumersEnabled: boolean;
}

export interface KafkaHandle {
  producer: Producer;
  consumer: Consumer | null;
  send: (topic: string, key: string, value: string) => Promise<void>;
}

export async function setupKafka(opts: KafkaSetupOptions): Promise<KafkaHandle> {
  const kafka = new Kafka({ clientId: "notification-service", brokers: opts.brokers });

  const producer = kafka.producer();
  await producer.connect();

  const send = async (topic: string, key: string, value: string): Promise<void> => {
    const record = stampXCanaryOnProducerRecord({
      topic,
      messages: [{ key, value }],
    });
    await producer.send(record);
  };

  let consumer: Consumer | null = null;
  if (opts.consumersEnabled) {
    consumer = kafka.consumer({ groupId: "notification-service" });
    await consumer.connect();
    await consumer.subscribe({ topics: ["orders.events", "payments.events"] });
    await consumer.run({
      eachMessage: async ({ topic, message }: EachMessagePayload) => {
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
      },
    });
    console.log("notification-service Kafka consumer subscribed to orders.events, payments.events");
  } else {
    console.log("KAFKA_CONSUMERS_ENABLED=false; consumer not started");
  }

  return { producer, consumer, send };
}
```

- [ ] **Step 2: Update `services/notification-service/src/index.ts` to wire Kafka and pass `send` to Restate**

```typescript
import { loadConfig } from "./config.js";
import { setupHttp, buildIngressClient } from "./http.js";
import { setupKafka } from "./kafka.js";
import { setupRestate, configureKafkaSend } from "./restate.js";

const config = loadConfig();

const ingressClient = buildIngressClient(config.RESTATE_INGRESS_URL);
const app = setupHttp({ ingressClient });

app.listen(config.HTTP_PORT, () => {
  console.log(`notification-service HTTP listening on ${config.HTTP_PORT}`);
});

const kafka = await setupKafka({
  brokers: config.KAFKA_BOOTSTRAP_SERVERS,
  consumersEnabled: config.KAFKA_CONSUMERS_ENABLED,
});

configureKafkaSend(kafka.send);

await setupRestate({
  registerHandlers: config.RESTATE_REGISTER_HANDLERS,
  port: config.RESTATE_HANDLER_PORT,
});
```

- [ ] **Step 3: Update `services/notification-service/src/restate.ts` to emit `notifications.events`**

Add a module-level `kafkaSend` variable + `configureKafkaSend` setter, and call it in the handler:

```typescript
import * as restate from "@restatedev/restate-sdk";
import { runWithCanary, applyXCanaryToRestateOptions } from "@canary/lib-node";
import {
  auditQueryServiceDef,
  notificationServiceDef,
  type Notification,
  type NotifyRequest,
  type AuditEvent,
} from "@canary/restate-defs-node";
import { notificationStore } from "./store.js";
import { randomUUID } from "node:crypto";

export interface RestateSetupOptions {
  registerHandlers: boolean;
  port: number;
}

export type KafkaSend = (topic: string, key: string, value: string) => Promise<void>;

let kafkaSend: KafkaSend | null = null;

export function configureKafkaSend(fn: KafkaSend): void {
  kafkaSend = fn;
}

export const notificationService = restate.service({
  name: notificationServiceDef.name,
  handlers: {
    notify: async (ctx: restate.Context, req: NotifyRequest): Promise<Notification> => {
      const incoming = (ctx as unknown as { request?: () => { headers?: Record<string, string> } }).request?.();
      const isCanary = incoming?.headers?.["x-canary"] === "true";

      return runWithCanary(isCanary, async () => {
        const notification: Notification = {
          id: randomUUID(),
          userId: req.userId,
          message: req.message,
          status: "sent",
        };
        notificationStore.put(notification);

        if (kafkaSend) {
          await kafkaSend("notifications.events", notification.id, JSON.stringify(notification));
        }

        const auditEvent: AuditEvent = {
          aggregate: "notification",
          id: notification.id,
          action: "sent",
          correlationId: req.orderId,
        };
        await ctx.serviceClient(auditQueryServiceDef).append(
          auditEvent,
          applyXCanaryToRestateOptions({}),
        );

        return notification;
      });
    },
  },
});

export async function setupRestate(opts: RestateSetupOptions): Promise<void> {
  if (!opts.registerHandlers) {
    console.log("RESTATE_REGISTER_HANDLERS=false; skipping Restate endpoint listener");
    return;
  }
  await restate.endpoint().bind(notificationService).listen(opts.port);
  console.log(`notification-service Restate handlers listening on ${opts.port}`);
}
```

- [ ] **Step 4: Update `restate.test.ts` to assert Kafka emission**

In the existing handler test, add a `configureKafkaSend` mock at the top of the file:

```typescript
import { setupRestate, notificationService, configureKafkaSend, type KafkaSend } from "../restate.js";
```

Add a new test:

```typescript
it("emits notifications.events via the configured kafkaSend", async () => {
  const kafkaSend = vi.fn().mockResolvedValue(undefined);
  configureKafkaSend(kafkaSend);

  const auditAppend = vi.fn().mockResolvedValue(undefined);
  const ctx = {
    request: () => ({ headers: {} }),
    serviceClient: vi.fn().mockReturnValue({ append: auditAppend }),
  };

  const handlers = (notificationService as unknown as {
    handlers: { notify: (ctx: unknown, req: unknown) => Promise<unknown> };
  }).handlers;

  const result = (await handlers.notify(
    ctx,
    { userId: "u_1", message: "hi", orderId: "ord_1" },
  )) as { id: string };

  expect(kafkaSend).toHaveBeenCalledWith(
    "notifications.events",
    result.id,
    expect.stringContaining(result.id),
  );
});
```

After this test, also reset `configureKafkaSend(null as unknown as KafkaSend)` in a cleanup hook so other tests don't see the mock (or wrap each test's setup in a fresh mock).

- [ ] **Step 5: Write the failing kafka.ts test (consumer gating + producer wrapping)**

`services/notification-service/src/__tests__/kafka.test.ts`:

```typescript
import { describe, it, expect, vi, beforeEach } from "vitest";

const connectMock = vi.fn().mockResolvedValue(undefined);
const sendMock = vi.fn().mockResolvedValue(undefined);
const subscribeMock = vi.fn().mockResolvedValue(undefined);
const runMock = vi.fn().mockResolvedValue(undefined);
const consumerConnectMock = vi.fn().mockResolvedValue(undefined);

vi.mock("kafkajs", () => {
  return {
    Kafka: vi.fn().mockImplementation(() => ({
      producer: () => ({ connect: connectMock, send: sendMock }),
      consumer: () => ({
        connect: consumerConnectMock,
        subscribe: subscribeMock,
        run: runMock,
      }),
    })),
  };
});

import { setupKafka } from "../kafka.js";

describe("setupKafka", () => {
  beforeEach(() => {
    connectMock.mockClear();
    sendMock.mockClear();
    subscribeMock.mockClear();
    runMock.mockClear();
    consumerConnectMock.mockClear();
  });

  it("connects producer always", async () => {
    await setupKafka({ brokers: ["localhost:9092"], consumersEnabled: false });
    expect(connectMock).toHaveBeenCalledOnce();
  });

  it("does NOT call consumer.subscribe / consumer.run when consumersEnabled=false", async () => {
    await setupKafka({ brokers: ["localhost:9092"], consumersEnabled: false });
    expect(subscribeMock).not.toHaveBeenCalled();
    expect(runMock).not.toHaveBeenCalled();
  });

  it("subscribes to orders.events + payments.events when consumersEnabled=true", async () => {
    await setupKafka({ brokers: ["localhost:9092"], consumersEnabled: true });
    expect(subscribeMock).toHaveBeenCalledWith({
      topics: ["orders.events", "payments.events"],
    });
    expect(runMock).toHaveBeenCalledOnce();
  });

  it("send() wraps records via stampXCanaryOnProducerRecord (calls producer.send)", async () => {
    const kafka = await setupKafka({ brokers: ["localhost:9092"], consumersEnabled: false });
    await kafka.send("notifications.events", "n_1", "{}");

    expect(sendMock).toHaveBeenCalledOnce();
    const arg = sendMock.mock.calls[0][0];
    expect(arg.topic).toBe("notifications.events");
    expect(arg.messages[0].key).toBe("n_1");
    expect(arg.messages[0].value).toBe("{}");
    // Without canary context, headers should be absent or empty.
  });
});
```

- [ ] **Step 6: Run all notification-service tests**

Run: `pnpm --filter @canary/notification-service test`

Expected: ~17 tests pass (5 config + 3 HTTP + 2 setupRestate gating + 3 handler + 4 kafka).

- [ ] **Step 7: Commit**

```bash
git add services/notification-service/
git commit -m "feat(notification-service): Kafka producer/consumer (gated) + notifications.events emission"
```

notification-service is now complete.

---

## Phase F — order-service (Node + Express, the saga exception)

order-service is the single exception to option β: its HTTP controller does HTTP fan-out directly to inventory, payment, notification (saga without compensation per trimmed-C). `CheckoutSaga` `@Workflow` is registered with a stub body — present so `RESTATE_REGISTER_HANDLERS` has something to gate. Phase 3 moves the saga inside `CheckoutSaga`.

### Task 20: Scaffold `services/order-service` pnpm package

**Files:**
- Create: `services/order-service/package.json`
- Create: `services/order-service/tsconfig.json`
- Create: `services/order-service/vitest.config.ts`
- Create: `services/order-service/.env.example`
- Create: `services/order-service/src/config.ts`
- Create: `services/order-service/src/store.ts`
- Create: `services/order-service/src/index.ts`
- Create: `services/order-service/src/__tests__/config.test.ts`

- [ ] **Step 1: Write `services/order-service/package.json`**

```json
{
  "name": "@canary/order-service",
  "version": "0.0.1",
  "private": true,
  "type": "module",
  "main": "./dist/index.js",
  "scripts": {
    "build": "tsc -p tsconfig.json",
    "test": "vitest run",
    "test:watch": "vitest",
    "start": "node dist/index.js"
  },
  "dependencies": {
    "@canary/lib-node": "workspace:*",
    "@canary/restate-defs-node": "workspace:*",
    "@restatedev/restate-sdk": "^1.14.2",
    "axios": "^1.7.7",
    "express": "^4.21.0",
    "kafkajs": "^2.2.4"
  },
  "devDependencies": {
    "@types/express": "^5.0.0",
    "@types/node": "^22.7.4",
    "@types/supertest": "^6.0.2",
    "supertest": "^7.0.0",
    "typescript": "^5.6.2",
    "vitest": "^2.1.1"
  }
}
```

- [ ] **Step 2: Write `services/order-service/tsconfig.json`**

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ESNext",
    "moduleResolution": "Bundler",
    "lib": ["ES2022"],
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "outDir": "./dist",
    "rootDir": "./src",
    "declaration": false,
    "resolveJsonModule": true,
    "isolatedModules": true
  },
  "include": ["src/**/*.ts"],
  "exclude": ["src/**/__tests__/**", "dist", "node_modules"]
}
```

- [ ] **Step 3: Write `services/order-service/vitest.config.ts`**

```typescript
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["src/**/__tests__/**/*.test.ts"],
    environment: "node",
  },
});
```

- [ ] **Step 4: Write `services/order-service/.env.example`**

```
PORT=3001
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_CONSUMERS_ENABLED=true
RESTATE_INGRESS_URL=http://localhost:9070
RESTATE_REGISTER_HANDLERS=true
RESTATE_HANDLER_PORT=9084
INVENTORY_URL=http://localhost:8082
PAYMENT_URL=http://localhost:8081
NOTIFICATION_URL=http://localhost:3002
```

- [ ] **Step 5: Write `services/order-service/src/config.ts`**

```typescript
export interface AppConfig {
  HTTP_PORT: number;
  KAFKA_BOOTSTRAP_SERVERS: string[];
  KAFKA_CONSUMERS_ENABLED: boolean;
  RESTATE_INGRESS_URL: string;
  RESTATE_REGISTER_HANDLERS: boolean;
  RESTATE_HANDLER_PORT: number;
  INVENTORY_URL: string;
  PAYMENT_URL: string;
  NOTIFICATION_URL: string;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  return {
    HTTP_PORT: Number(env.PORT ?? 3001),
    KAFKA_BOOTSTRAP_SERVERS: (env.KAFKA_BOOTSTRAP_SERVERS ?? "localhost:9092").split(","),
    KAFKA_CONSUMERS_ENABLED: env.KAFKA_CONSUMERS_ENABLED !== "false",
    RESTATE_INGRESS_URL: env.RESTATE_INGRESS_URL ?? "http://localhost:9070",
    RESTATE_REGISTER_HANDLERS: env.RESTATE_REGISTER_HANDLERS !== "false",
    RESTATE_HANDLER_PORT: Number(env.RESTATE_HANDLER_PORT ?? 9084),
    INVENTORY_URL: env.INVENTORY_URL ?? "http://localhost:8082",
    PAYMENT_URL: env.PAYMENT_URL ?? "http://localhost:8081",
    NOTIFICATION_URL: env.NOTIFICATION_URL ?? "http://localhost:3002",
  };
}
```

- [ ] **Step 6: Write the failing config test**

`services/order-service/src/__tests__/config.test.ts`:

```typescript
import { describe, it, expect } from "vitest";
import { loadConfig } from "../config.js";

describe("loadConfig", () => {
  it("uses defaults when env is empty", () => {
    const cfg = loadConfig({});
    expect(cfg.HTTP_PORT).toBe(3001);
    expect(cfg.RESTATE_HANDLER_PORT).toBe(9084);
    expect(cfg.INVENTORY_URL).toBe("http://localhost:8082");
    expect(cfg.PAYMENT_URL).toBe("http://localhost:8081");
    expect(cfg.NOTIFICATION_URL).toBe("http://localhost:3002");
    expect(cfg.KAFKA_CONSUMERS_ENABLED).toBe(true);
    expect(cfg.RESTATE_REGISTER_HANDLERS).toBe(true);
  });

  it("respects explicit overrides", () => {
    const cfg = loadConfig({
      INVENTORY_URL: "http://inventory.svc:8080",
      PAYMENT_URL: "http://payment.svc:8080",
      NOTIFICATION_URL: "http://notification.svc:8080",
      KAFKA_CONSUMERS_ENABLED: "false",
      RESTATE_REGISTER_HANDLERS: "false",
    });
    expect(cfg.INVENTORY_URL).toBe("http://inventory.svc:8080");
    expect(cfg.PAYMENT_URL).toBe("http://payment.svc:8080");
    expect(cfg.NOTIFICATION_URL).toBe("http://notification.svc:8080");
    expect(cfg.KAFKA_CONSUMERS_ENABLED).toBe(false);
    expect(cfg.RESTATE_REGISTER_HANDLERS).toBe(false);
  });
});
```

- [ ] **Step 7: Write `services/order-service/src/store.ts`**

```typescript
import type { Order } from "@canary/restate-defs-node";

export interface ConsumedEvent {
  topic: string;
  key: string | null;
  value: string;
  headers: Record<string, string>;
}

class OrderStore {
  private byId = new Map<string, Order>();

  put(o: Order): void {
    this.byId.set(o.id, o);
  }

  findById(id: string): Order | undefined {
    return this.byId.get(id);
  }
}

class ConsumedEventStore {
  private events: ConsumedEvent[] = [];

  record(e: ConsumedEvent): void {
    this.events.push(e);
  }

  all(): ConsumedEvent[] {
    return [...this.events];
  }
}

export const orderStore = new OrderStore();
export const consumedEventStore = new ConsumedEventStore();
```

- [ ] **Step 8: Write `services/order-service/src/index.ts` skeleton**

```typescript
import { loadConfig } from "./config.js";

const config = loadConfig();

console.log("order-service booting", {
  httpPort: config.HTTP_PORT,
  restateRegisterHandlers: config.RESTATE_REGISTER_HANDLERS,
  kafkaConsumersEnabled: config.KAFKA_CONSUMERS_ENABLED,
});

// HTTP, Kafka, Restate setup are wired in Tasks 21, 22, 23.
```

- [ ] **Step 9: Run install + tests**

Run: `pnpm install`
Run: `pnpm --filter @canary/order-service test`

Expected: 2 config tests pass.

- [ ] **Step 10: Commit**

```bash
git add pnpm-lock.yaml services/order-service/
git commit -m "feat(order-service): scaffold pnpm package + config + store"
```

---

### Task 21: HTTP routes + saga (HTTP fan-out from controller, no Ingress delegation)

**Files:**
- Create: `services/order-service/src/saga.ts`
- Create: `services/order-service/src/http.ts`
- Modify: `services/order-service/src/index.ts`
- Create: `services/order-service/src/__tests__/saga.test.ts`
- Create: `services/order-service/src/__tests__/http.test.ts`

- [ ] **Step 1: Write `services/order-service/src/saga.ts`**

```typescript
import type { AxiosInstance } from "axios";
import type {
  OrderRequest,
  Reservation,
  ReservationRequest,
  Charge,
  ChargeRequest,
  Notification,
  NotifyRequest,
} from "@canary/restate-defs-node";

export interface SagaClients {
  inventory: AxiosInstance;
  payment: AxiosInstance;
  notification: AxiosInstance;
}

export interface SagaResult {
  reservation: Reservation;
  charge: Charge;
  notification: Notification;
}

/**
 * Sequential HTTP fan-out (saga without compensation per trimmed-C). Phase 3
 * will move this inside the CheckoutSaga workflow with proper compensation.
 *
 * Each axios client is configured with attachXCanaryAxiosInterceptor by the
 * caller (see http.ts) so x-canary propagates automatically.
 */
export async function runSaga(orderId: string, req: OrderRequest, clients: SagaClients): Promise<SagaResult> {
  const reservationReq: ReservationRequest = {
    sku: req.sku,
    quantity: req.quantity,
    orderId,
  };
  const reservation = (await clients.inventory.post<Reservation>("/reservations", reservationReq)).data;

  const chargeReq: ChargeRequest = { orderId, amount: req.amount };
  const charge = (await clients.payment.post<Charge>("/charges", chargeReq)).data;

  const notifyReq: NotifyRequest = {
    userId: req.userId,
    message: `Order ${orderId} confirmed`,
    orderId,
  };
  const notification = (await clients.notification.post<Notification>("/notifications", notifyReq)).data;

  return { reservation, charge, notification };
}
```

- [ ] **Step 2: Write the failing saga test**

`services/order-service/src/__tests__/saga.test.ts`:

```typescript
import { describe, it, expect, vi } from "vitest";
import type { AxiosInstance } from "axios";
import { runSaga, type SagaClients } from "../saga.js";

function mockAxios(response: unknown): AxiosInstance {
  return { post: vi.fn().mockResolvedValue({ data: response }) } as unknown as AxiosInstance;
}

describe("runSaga", () => {
  it("calls inventory, payment, and notification in sequence with the right bodies", async () => {
    const reservation = { id: "r_1", sku: "widget", quantity: 1, orderId: "ord_1", status: "reserved" };
    const charge = { id: "ch_1", orderId: "ord_1", amount: 100, status: "succeeded" };
    const notification = { id: "n_1", userId: "u_1", message: "Order ord_1 confirmed", status: "sent" };

    const clients: SagaClients = {
      inventory: mockAxios(reservation),
      payment: mockAxios(charge),
      notification: mockAxios(notification),
    };

    const result = await runSaga("ord_1", { userId: "u_1", sku: "widget", quantity: 1, amount: 100 }, clients);

    expect(result).toEqual({ reservation, charge, notification });
    expect(clients.inventory.post).toHaveBeenCalledWith("/reservations", {
      sku: "widget",
      quantity: 1,
      orderId: "ord_1",
    });
    expect(clients.payment.post).toHaveBeenCalledWith("/charges", { orderId: "ord_1", amount: 100 });
    expect(clients.notification.post).toHaveBeenCalledWith("/notifications", {
      userId: "u_1",
      message: "Order ord_1 confirmed",
      orderId: "ord_1",
    });
  });

  it("propagates the first downstream error (no compensation in 1.3.a)", async () => {
    const failing = { post: vi.fn().mockRejectedValue(new Error("inventory down")) } as unknown as AxiosInstance;
    const clients: SagaClients = {
      inventory: failing,
      payment: mockAxios(null),
      notification: mockAxios(null),
    };

    await expect(
      runSaga("ord_1", { userId: "u_1", sku: "widget", quantity: 1, amount: 100 }, clients),
    ).rejects.toThrow("inventory down");
  });
});
```

- [ ] **Step 3: Write `services/order-service/src/http.ts`**

```typescript
import express, { type Express } from "express";
import axios, { type AxiosInstance } from "axios";
import { xCanaryMiddleware, attachXCanaryAxiosInterceptor } from "@canary/lib-node";
import type { Order, OrderRequest } from "@canary/restate-defs-node";
import { orderStore, consumedEventStore } from "./store.js";
import { runSaga, type SagaClients } from "./saga.js";
import { randomUUID } from "node:crypto";

export interface HttpDeps {
  clients: SagaClients;
  kafkaSend?: (topic: string, key: string, value: string) => Promise<void>;
}

export function buildClient(baseURL: string): AxiosInstance {
  const client = axios.create({ baseURL });
  attachXCanaryAxiosInterceptor(client);
  return client;
}

export function setupHttp(deps: HttpDeps): Express {
  const app = express();
  app.use(express.json());
  app.use(xCanaryMiddleware());

  app.post("/api/orders", async (req, res) => {
    const body = req.body as OrderRequest;
    const orderId = randomUUID();

    const initial: Order = {
      id: orderId,
      userId: body.userId,
      sku: body.sku,
      quantity: body.quantity,
      amount: body.amount,
      status: "pending",
    };
    orderStore.put(initial);

    if (deps.kafkaSend) {
      await deps.kafkaSend("orders.events", orderId, JSON.stringify(initial));
    }

    try {
      await runSaga(orderId, body, deps.clients);
      const completed: Order = { ...initial, status: "completed" };
      orderStore.put(completed);
      res.status(201).json(completed);
    } catch (err) {
      const failed: Order = { ...initial, status: "failed" };
      orderStore.put(failed);
      console.error("saga failed", err);
      res.status(502).json({ error: "saga_failed", order: failed });
    }
  });

  app.get("/api/orders/:id", (req, res) => {
    const order = orderStore.findById(req.params.id);
    if (!order) {
      res.status(404).end();
      return;
    }
    res.json(order);
  });

  app.get("/internal/consumed-events", (_req, res) => {
    res.json(consumedEventStore.all());
  });

  return app;
}
```

- [ ] **Step 4: Update `services/order-service/src/index.ts` to wire HTTP**

```typescript
import { loadConfig } from "./config.js";
import { setupHttp, buildClient } from "./http.js";

const config = loadConfig();

const clients = {
  inventory: buildClient(config.INVENTORY_URL),
  payment: buildClient(config.PAYMENT_URL),
  notification: buildClient(config.NOTIFICATION_URL),
};

const app = setupHttp({ clients });

app.listen(config.HTTP_PORT, () => {
  console.log(`order-service HTTP listening on ${config.HTTP_PORT}`);
});

// Kafka + Restate setup are wired in Tasks 22 + 23.
```

- [ ] **Step 5: Write the failing HTTP test**

`services/order-service/src/__tests__/http.test.ts`:

```typescript
import { describe, it, expect, beforeEach, vi } from "vitest";
import request from "supertest";
import type { AxiosInstance } from "axios";
import { setupHttp } from "../http.js";
import { orderStore } from "../store.js";

function mockAxios(response: unknown): AxiosInstance {
  return { post: vi.fn().mockResolvedValue({ data: response }) } as unknown as AxiosInstance;
}

describe("HTTP routes", () => {
  beforeEach(() => {
    (orderStore as unknown as { byId: Map<string, unknown> }).byId.clear();
  });

  it("POST /api/orders runs the saga and returns the completed order", async () => {
    const app = setupHttp({
      clients: {
        inventory: mockAxios({ id: "r_1", sku: "widget", quantity: 1, orderId: "?", status: "reserved" }),
        payment: mockAxios({ id: "ch_1", orderId: "?", amount: 100, status: "succeeded" }),
        notification: mockAxios({ id: "n_1", userId: "u_1", message: "x", status: "sent" }),
      },
    });

    const res = await request(app)
      .post("/api/orders")
      .send({ userId: "u_1", sku: "widget", quantity: 1, amount: 100 });

    expect(res.status).toBe(201);
    expect(res.body.status).toBe("completed");
    expect(res.body.userId).toBe("u_1");
    expect(orderStore.findById(res.body.id)?.status).toBe("completed");
  });

  it("POST /api/orders returns 502 + status=failed on downstream failure", async () => {
    const failing = { post: vi.fn().mockRejectedValue(new Error("inventory down")) } as unknown as AxiosInstance;
    const app = setupHttp({
      clients: {
        inventory: failing,
        payment: mockAxios(null),
        notification: mockAxios(null),
      },
    });

    const res = await request(app)
      .post("/api/orders")
      .send({ userId: "u_1", sku: "widget", quantity: 1, amount: 100 });

    expect(res.status).toBe(502);
    expect(res.body.error).toBe("saga_failed");
    expect(res.body.order.status).toBe("failed");
  });

  it("emits orders.events via kafkaSend when configured", async () => {
    const kafkaSend = vi.fn().mockResolvedValue(undefined);
    const app = setupHttp({
      clients: {
        inventory: mockAxios({ id: "r_1", sku: "widget", quantity: 1, orderId: "?", status: "reserved" }),
        payment: mockAxios({ id: "ch_1", orderId: "?", amount: 100, status: "succeeded" }),
        notification: mockAxios({ id: "n_1", userId: "u_1", message: "x", status: "sent" }),
      },
      kafkaSend,
    });

    const res = await request(app)
      .post("/api/orders")
      .send({ userId: "u_1", sku: "widget", quantity: 1, amount: 100 });

    expect(res.status).toBe(201);
    expect(kafkaSend).toHaveBeenCalledOnce();
    const [topic, key, value] = kafkaSend.mock.calls[0];
    expect(topic).toBe("orders.events");
    expect(key).toBe(res.body.id);
    expect(value).toContain(res.body.id);
  });

  it("GET /api/orders/:id returns 200 when found", async () => {
    orderStore.put({ id: "ord_1", userId: "u_1", sku: "widget", quantity: 1, amount: 100, status: "completed" });

    const app = setupHttp({
      clients: { inventory: mockAxios(null), payment: mockAxios(null), notification: mockAxios(null) },
    });

    const res = await request(app).get("/api/orders/ord_1");

    expect(res.status).toBe(200);
    expect(res.body.id).toBe("ord_1");
  });

  it("GET /api/orders/:id returns 404 when missing", async () => {
    const app = setupHttp({
      clients: { inventory: mockAxios(null), payment: mockAxios(null), notification: mockAxios(null) },
    });

    const res = await request(app).get("/api/orders/nope");

    expect(res.status).toBe(404);
  });
});
```

- [ ] **Step 6: Run all order-service tests**

Run: `pnpm --filter @canary/order-service test`

Expected: 2 config + 2 saga + 5 HTTP = 9 tests pass.

- [ ] **Step 7: Commit**

```bash
git add services/order-service/
git commit -m "feat(order-service): HTTP fan-out saga + GET /api/orders/{id}"
```

---

### Task 22: CheckoutSaga stub Restate handler + endpoint setup (gated)

**Files:**
- Create: `services/order-service/src/restate.ts`
- Modify: `services/order-service/src/index.ts`
- Create: `services/order-service/src/__tests__/restate.test.ts`

- [ ] **Step 1: Write `services/order-service/src/restate.ts`**

```typescript
import * as restate from "@restatedev/restate-sdk";
import { runWithCanary } from "@canary/lib-node";
import {
  checkoutSagaDef,
  type Order,
  type OrderRequest,
} from "@canary/restate-defs-node";
import { randomUUID } from "node:crypto";

export interface RestateSetupOptions {
  registerHandlers: boolean;
  port: number;
}

/**
 * 1.3.a stub: registered so the canary flag (RESTATE_REGISTER_HANDLERS) has
 * something to gate. Phase 3 fills in the actual saga logic with R-to-R calls
 * to PaymentVO, ReservationWorkflow, NotificationService.
 */
export const checkoutSaga = restate.workflow({
  name: checkoutSagaDef.name,
  handlers: {
    run: async (ctx: restate.WorkflowContext, req: OrderRequest): Promise<Order> => {
      const incoming = (ctx as unknown as { request?: () => { headers?: Record<string, string> } }).request?.();
      const isCanary = incoming?.headers?.["x-canary"] === "true";

      return runWithCanary(isCanary, async () => {
        // Phase 3 will replace this body with R-to-R calls to
        // PaymentVO, ReservationWorkflow, NotificationService.
        return {
          id: randomUUID(),
          userId: req.userId,
          sku: req.sku,
          quantity: req.quantity,
          amount: req.amount,
          status: "stub-completed",
        };
      });
    },
  },
});

export async function setupRestate(opts: RestateSetupOptions): Promise<void> {
  if (!opts.registerHandlers) {
    console.log("RESTATE_REGISTER_HANDLERS=false; skipping Restate endpoint listener");
    return;
  }
  await restate.endpoint().bind(checkoutSaga).listen(opts.port);
  console.log(`order-service Restate handlers listening on ${opts.port}`);
}
```

- [ ] **Step 2: Update `services/order-service/src/index.ts`**

```typescript
import { loadConfig } from "./config.js";
import { setupHttp, buildClient } from "./http.js";
import { setupRestate } from "./restate.js";

const config = loadConfig();

const clients = {
  inventory: buildClient(config.INVENTORY_URL),
  payment: buildClient(config.PAYMENT_URL),
  notification: buildClient(config.NOTIFICATION_URL),
};

const app = setupHttp({ clients });

app.listen(config.HTTP_PORT, () => {
  console.log(`order-service HTTP listening on ${config.HTTP_PORT}`);
});

await setupRestate({
  registerHandlers: config.RESTATE_REGISTER_HANDLERS,
  port: config.RESTATE_HANDLER_PORT,
});

// Kafka setup wired in Task 23.
```

- [ ] **Step 3: Write the failing Restate test**

`services/order-service/src/__tests__/restate.test.ts`:

```typescript
import { describe, it, expect, vi } from "vitest";
import { setupRestate, checkoutSaga } from "../restate.js";

vi.mock("@restatedev/restate-sdk", async () => {
  const actual = await vi.importActual<typeof import("@restatedev/restate-sdk")>(
    "@restatedev/restate-sdk",
  );
  return {
    ...actual,
    endpoint: vi.fn(() => ({
      bind: vi.fn().mockReturnThis(),
      listen: vi.fn().mockResolvedValue(undefined),
    })),
  };
});

describe("setupRestate gating", () => {
  it("does NOT call endpoint().listen when registerHandlers=false", async () => {
    const restate = await import("@restatedev/restate-sdk");
    (restate.endpoint as ReturnType<typeof vi.fn>).mockClear();

    await setupRestate({ registerHandlers: false, port: 9084 });

    expect(restate.endpoint).not.toHaveBeenCalled();
  });

  it("calls endpoint().bind(checkoutSaga).listen(port) when registerHandlers=true", async () => {
    const restate = await import("@restatedev/restate-sdk");
    const bindMock = vi.fn().mockReturnThis();
    const listenMock = vi.fn().mockResolvedValue(undefined);
    (restate.endpoint as ReturnType<typeof vi.fn>).mockReturnValue({
      bind: bindMock,
      listen: listenMock,
    });

    await setupRestate({ registerHandlers: true, port: 9084 });

    expect(restate.endpoint).toHaveBeenCalledOnce();
    expect(bindMock).toHaveBeenCalledWith(checkoutSaga);
    expect(listenMock).toHaveBeenCalledWith(9084);
  });
});

describe("CheckoutSaga.run handler stub", () => {
  it("returns a stub-completed Order", async () => {
    const ctx = {
      request: () => ({ headers: {} }),
    };

    const handlers = (checkoutSaga as unknown as {
      handlers: { run: (ctx: unknown, req: unknown) => Promise<unknown> };
    }).handlers;

    const result = (await handlers.run(ctx, {
      userId: "u_1",
      sku: "widget",
      quantity: 1,
      amount: 100,
    })) as { status: string; userId: string };

    expect(result.status).toBe("stub-completed");
    expect(result.userId).toBe("u_1");
  });
});
```

- [ ] **Step 4: Run all order-service tests**

Run: `pnpm --filter @canary/order-service test`

Expected: 2 config + 2 saga + 5 HTTP + 2 setupRestate gating + 1 stub handler = 12 tests pass.

- [ ] **Step 5: Commit**

```bash
git add services/order-service/
git commit -m "feat(order-service): CheckoutSaga stub Restate handler + endpoint setup (gated)"
```

---

### Task 23: Kafka producer (`orders.events`) + consumer (`payments.events`, `inventory.events`, gated) + `/internal/consumed-events`

**Files:**
- Create: `services/order-service/src/kafka.ts`
- Modify: `services/order-service/src/index.ts`
- Create: `services/order-service/src/__tests__/kafka.test.ts`

- [ ] **Step 1: Write `services/order-service/src/kafka.ts`**

```typescript
import { Kafka, Producer, Consumer, type EachMessagePayload } from "kafkajs";
import { stampXCanaryOnProducerRecord } from "@canary/lib-node";
import { consumedEventStore } from "./store.js";

export interface KafkaSetupOptions {
  brokers: string[];
  consumersEnabled: boolean;
}

export interface KafkaHandle {
  producer: Producer;
  consumer: Consumer | null;
  send: (topic: string, key: string, value: string) => Promise<void>;
}

export async function setupKafka(opts: KafkaSetupOptions): Promise<KafkaHandle> {
  const kafka = new Kafka({ clientId: "order-service", brokers: opts.brokers });

  const producer = kafka.producer();
  await producer.connect();

  const send = async (topic: string, key: string, value: string): Promise<void> => {
    const record = stampXCanaryOnProducerRecord({
      topic,
      messages: [{ key, value }],
    });
    await producer.send(record);
  };

  let consumer: Consumer | null = null;
  if (opts.consumersEnabled) {
    consumer = kafka.consumer({ groupId: "order-service" });
    await consumer.connect();
    await consumer.subscribe({ topics: ["payments.events", "inventory.events"] });
    await consumer.run({
      eachMessage: async ({ topic, message }: EachMessagePayload) => {
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
      },
    });
    console.log("order-service Kafka consumer subscribed to payments.events, inventory.events");
  } else {
    console.log("KAFKA_CONSUMERS_ENABLED=false; consumer not started");
  }

  return { producer, consumer, send };
}
```

- [ ] **Step 2: Update `services/order-service/src/index.ts` to wire Kafka and pass send to setupHttp**

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
});

const app = setupHttp({ clients, kafkaSend: kafka.send });

app.listen(config.HTTP_PORT, () => {
  console.log(`order-service HTTP listening on ${config.HTTP_PORT}`);
});

await setupRestate({
  registerHandlers: config.RESTATE_REGISTER_HANDLERS,
  port: config.RESTATE_HANDLER_PORT,
});
```

- [ ] **Step 3: Write the failing kafka.ts test**

`services/order-service/src/__tests__/kafka.test.ts`:

```typescript
import { describe, it, expect, vi, beforeEach } from "vitest";

const connectMock = vi.fn().mockResolvedValue(undefined);
const sendMock = vi.fn().mockResolvedValue(undefined);
const subscribeMock = vi.fn().mockResolvedValue(undefined);
const runMock = vi.fn().mockResolvedValue(undefined);
const consumerConnectMock = vi.fn().mockResolvedValue(undefined);

vi.mock("kafkajs", () => {
  return {
    Kafka: vi.fn().mockImplementation(() => ({
      producer: () => ({ connect: connectMock, send: sendMock }),
      consumer: () => ({
        connect: consumerConnectMock,
        subscribe: subscribeMock,
        run: runMock,
      }),
    })),
  };
});

import { setupKafka } from "../kafka.js";

describe("setupKafka (order-service)", () => {
  beforeEach(() => {
    connectMock.mockClear();
    sendMock.mockClear();
    subscribeMock.mockClear();
    runMock.mockClear();
    consumerConnectMock.mockClear();
  });

  it("connects producer always", async () => {
    await setupKafka({ brokers: ["localhost:9092"], consumersEnabled: false });
    expect(connectMock).toHaveBeenCalledOnce();
  });

  it("does NOT subscribe / run consumer when consumersEnabled=false", async () => {
    await setupKafka({ brokers: ["localhost:9092"], consumersEnabled: false });
    expect(subscribeMock).not.toHaveBeenCalled();
    expect(runMock).not.toHaveBeenCalled();
  });

  it("subscribes to payments.events + inventory.events when consumersEnabled=true", async () => {
    await setupKafka({ brokers: ["localhost:9092"], consumersEnabled: true });
    expect(subscribeMock).toHaveBeenCalledWith({
      topics: ["payments.events", "inventory.events"],
    });
    expect(runMock).toHaveBeenCalledOnce();
  });

  it("send() forwards orders.events through producer.send (with stamping wrapper applied)", async () => {
    const kafka = await setupKafka({ brokers: ["localhost:9092"], consumersEnabled: false });
    await kafka.send("orders.events", "ord_1", "{}");

    expect(sendMock).toHaveBeenCalledOnce();
    const arg = sendMock.mock.calls[0][0];
    expect(arg.topic).toBe("orders.events");
    expect(arg.messages[0].key).toBe("ord_1");
    expect(arg.messages[0].value).toBe("{}");
  });
});
```

- [ ] **Step 4: Run all order-service tests**

Run: `pnpm --filter @canary/order-service test`

Expected: 2 config + 2 saga + 5 HTTP + 2 setupRestate gating + 1 stub handler + 4 kafka = 16 tests pass.

- [ ] **Step 5: Commit**

```bash
git add services/order-service/
git commit -m "feat(order-service): Kafka producer/consumer (gated) + orders.events emission"
```

order-service complete.

---

## Phase G — Build/Make integration

### Task 24: Update `make verify`, add `make build-services`, refresh README

**Files:**
- Modify: `Makefile`
- Modify: `README.md`

- [ ] **Step 1: Update `make verify` to run all Java + Node tests**

Replace the existing `verify` target in `Makefile`:

```makefile
verify: ## Run all unit/library/service tests (Java + Node)
	@echo "==> Java"
	@./gradlew test --quiet
	@echo "==> Node"
	@pnpm -r --filter "@canary/*" run test
```

`./gradlew test` runs tests across all Gradle subprojects (lib-java, restate-defs-java, all 3 services). `pnpm -r --filter "@canary/*" run test` runs tests in every workspace package that defines a `test` script (lib-node, both Node services; restate-defs-node has no test script and is skipped).

- [ ] **Step 2: Add `make build-services` target**

Add to `Makefile` after the `verify` target:

```makefile
build-services: ## Compile all 5 service binaries (Java bootJars + Node tsc dist)
	@echo "==> Java services"
	@./gradlew :services:audit-service:bootJar :services:payment-service:bootJar :services:inventory-service:bootJar --quiet
	@echo "==> Node services"
	@pnpm --filter @canary/order-service build
	@pnpm --filter @canary/notification-service build
```

Add `build-services` to the `.PHONY` list at the top of the Makefile:

```makefile
.PHONY: help up down status smoke-infra dashboards dashboards-stop dashboards-status verify build-services clean
```

- [ ] **Step 3: Run `make verify` to confirm everything passes**

Run: `make verify`

Expected: BUILD SUCCESSFUL across all Gradle subprojects (~13+10+10+16 = ~49 Java tests including lib-java's existing ~22) and all pnpm packages (~17+16 = ~33 Node tests including lib-node's existing ~14). Total ~80+ tests.

If anything fails, fix before continuing — do not proceed to Step 4 with red tests.

- [ ] **Step 4: Run `make build-services` to confirm the build chain produces artifacts**

Run: `make build-services`

Expected:
- 3 Java bootJars produced under `services/{audit,payment,inventory}-service/build/libs/`
- Node services compile cleanly: `services/{order,notification}-service/dist/index.js` exists.

- [ ] **Step 5: Update README.md — add the 1.3.a section**

Append after the "Plan 1.2" section (before any `Next phase:` line — replace the existing trailing "Next phase: 1.3..." with this):

```markdown
## Plan 1.3.a — Domain services code (complete)

Five domain services live under `services/`:

| Service | Stack | HTTP port (local) | Restate port | Kafka producer | Kafka consumer |
|---|---|---|---|---|---|
| order-service | TS + Node | 3001 | 9084 | `orders.events` | `payments.events`, `inventory.events` |
| payment-service | Java + Spring Boot | 8081 | 9081 | `payments.events` | `orders.events` |
| inventory-service | Java + Spring Boot | 8082 | 9082 | `inventory.events` | `orders.events` |
| notification-service | TS + Node | 3002 | 9085 | `notifications.events` | `orders.events`, `payments.events` |
| audit-service | Java + Spring Boot | 8083 | 9083 | `audit.events` | all `*.events` |

Two new shared modules carry cross-service Restate type contracts:
- `platform/restate-defs-java` — DTOs + abstract `@Service`/`@VirtualObject`/`@Workflow` definitions.
- `platform/restate-defs-node` — TS DTOs + `restate.ServiceDefinition`-style defs.

Per-service feature flags (set false on canary pods in 1.3.b):
- `KAFKA_CONSUMERS_ENABLED` — gates `@KafkaListener` (Java) / `consumer.subscribe` (Node).
- `RESTATE_REGISTER_HANDLERS` — gates the Restate HTTP endpoint listener.

Build / test:

| Command | What it does |
|---|---|
| `make verify` | Run all Java + Node unit tests (~80+ tests) |
| `make build-services` | Compile all 5 service binaries (Java bootJars + Node `tsc` dist) |
| `./gradlew :services:<name>:test` | Java service tests in isolation |
| `pnpm --filter @canary/<name> test` | Node service tests in isolation |

**Phase 1.3.a is code only — no deployment artifacts.** Dockerfiles, KafkaTopic CRDs, k8s manifests, image build scripts, and the canary Helm overlay are 1.3.b.

Next phase: 1.3.b (deployment artifacts so the services run on the kind cluster from Plan 1.1).
```

Also add `make build-services` to the existing operator-workflow command table near the top of the README.

- [ ] **Step 6: Commit**

```bash
git add Makefile README.md
git commit -m "feat(build): add build-services target; update verify to run all subprojects; document 1.3.a in README"
```

---

## Wrap-up

After Task 24, `make verify` is green, all 5 services build, and the README documents the new state. The branch is ready for the **finishing-a-development-branch** skill to merge back to `main`.

Final invariant checks before merge:
- `make verify` green (no flaky tests, no skipped tests)
- `make build-services` produces all 5 artifacts
- No `TODO` or `FIXME` left in service code (only intentional Phase 3 deferrals via the `CheckoutSaga` stub body and the inventory `ReservationWorkflow` "no timer" comment)
- `git log --oneline main..HEAD` shows ~24 focused commits, one per task

