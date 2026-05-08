# Canary Release Management — Plan 1.3.a Design (Domain Services Code)

**Status:** Draft — awaiting user review
**Date:** 2026-05-08
**Phase:** 1, sub-plan 1.3.a

This is the implementation design for **Plan 1.3.a — Domain services code**, the
first half of the 1.3 sub-phase split. It implements the 5 domain services
described in the [Phase 1 design](2026-05-08-canary-release-phase-1-design.md)
with thin in-memory state. Pure unit tests, all I/O mocked. **No deployment
artifacts** — Dockerfiles, KafkaTopic CRDs, k8s manifests, image scripts, and
Helm overlays are 1.3.b.

## Goal

Ship 5 domain services that:

1. Expose the HTTP surface defined in the Phase 1 design (lines 149-153, 156-165).
2. Produce per-service Kafka events on `<service>.events` topics; consume per the
   call graph; persist consumed events to an in-memory list for later e2e
   assertions.
3. Register Restate handlers using the per-service SDK type called out in the
   spec (`@VirtualObject` payment, `@Workflow` inventory + order, `@Service`
   notification + audit).
4. Propagate `x-canary: true` through every outbound surface (HTTP, Kafka,
   Restate) using primitives already shipped in `platform/lib-java` (1.2) and
   `platform/lib-node` (1.2).
5. Gate Kafka consumer subscription and Restate handler exposure on two boolean
   feature flags (`KAFKA_CONSUMERS_ENABLED`, `RESTATE_REGISTER_HANDLERS`) so
   that 1.3.b's canary overlay can flip pods into substrate-isolation mode.

## Non-goals (1.3.a)

| Item | Where it lands |
|---|---|
| Dockerfiles per service | 1.3.b |
| Strimzi `KafkaTopic` CRDs (topic provisioning) | 1.3.b |
| K8s manifests / Helm `service-chart` / canary values overlay | 1.3.b |
| `make build-images`, `make load-images`, `make deploy-services` | 1.3.b |
| Health endpoints (`/actuator/health`, `GET /health`) — needed for k8s probes | 1.3.b |
| Per-service Istio `DestinationRule` / `VirtualService` (default rule only) | 1.4 |
| Canary `Deployment` overlay (with both flags=false) | 1.4 |
| `canary-ctl` CLI | 1.4 |
| 13 e2e acceptance scenarios | 1.5 |
| Inventory `ReservationWorkflow` timer + release-on-expiry | Phase 3 |
| Order `CheckoutSaga` Restate-to-Restate fan-out + compensation | Phase 3 |
| Restate handler versioning | Phase 3 |
| Kafka header-routed consumption (route by `x-canary`) | Phase 2 |
| Schema registry / Avro | Phase 2 |
| OPA/Kyverno admission policies | Phase 4 |
| GitHub Actions CI | Phase 4 |
| OpenTelemetry, Grafana dashboards, alerting | Phase 5 |
| AuthN / AuthZ | non-goal for this reference (entire project) |
| Persistent stores (Postgres/Redis instead of in-memory) | non-goal for this reference (entire project) |

## Architecture & service inventory

Five new services under `services/<name>/`. Java services are Gradle subprojects;
Node services are pnpm workspace packages.

```
services/
├── order-service/          # TS + Node + Express + KafkaJS
├── payment-service/        # Java + Spring Boot 4
├── inventory-service/      # Java + Spring Boot 4
├── notification-service/   # TS + Node + Express + KafkaJS
└── audit-service/          # Java + Spring Boot 4
```

### Common service-component shape

| Component | Java implementation | Node implementation |
|---|---|---|
| HTTP entry | Spring `@RestController`s; inbound `x-canary` via `XCanaryRequestFilter` (lib-java, 1.2) | Express routers; inbound via `xCanaryMiddleware` (lib-node, 1.2) |
| In-memory store | `ConcurrentHashMap<String, Record>` bean | module-level `Map<string, Record>` |
| Kafka producer | `KafkaTemplate<String, String>` + `XCanaryKafkaProducerInterceptor` | KafkaJS `producer` + `stampXCanaryOnProducerRecord` |
| Kafka consumer (gated by `KAFKA_CONSUMERS_ENABLED`) | `@KafkaListener` bean with `@ConditionalOnProperty` | KafkaJS `consumer.run()` skipped at boot if flag false |
| Restate handler (gated by `RESTATE_REGISTER_HANDLERS`) | `@VirtualObject` / `@Workflow` / `@Service` SDK type per spec; HTTP endpoint via Restate Java SDK 2.7 (`sdk-http-vertx`) | `restate.endpoint().bind(...)` per spec; HTTP/2 listener skipped at boot if flag false |
| Restate Ingress client (HTTP controller delegates here) | `RestClient` configured for Restate Ingress URL; x-canary stamped automatically via the existing `XCanaryRestClientInterceptor` (1.2) | axios configured for Restate Ingress URL; x-canary stamped automatically via the existing `attachXCanaryAxiosInterceptor` (1.2) |

**Order-svc is the exception**: its `CheckoutSaga` workflow is a stub in 1.3.a, so order-svc's HTTP controller does HTTP fan-out directly to inventory/payment/notification (no Ingress delegation). Phase 3 will move the saga inside `CheckoutSaga`. All four other services use the Ingress-delegation pattern below.

### Per-service detail

| Service | HTTP | Restate type | What handler does in 1.3.a | Kafka producer | Kafka consumer |
|---|---|---|---|---|---|
| order | `POST /api/orders`, `GET /api/orders/{id}` | `@Workflow CheckoutSaga` | **stub body** — registered so the canary flag has something to gate; saga semantics are Phase 3. The HTTP fan-out lives in the controller. | `orders.events` | `payments.events`, `inventory.events` |
| payment | `POST /charges`, `GET /charges/{id}` | `@VirtualObject PaymentVO` keyed by orderId | idempotent: return existing state if set, else write + return; calls audit handler via Restate-to-Restate | `payments.events` | `orders.events` |
| inventory | `POST /reservations`, `GET /products/{sku}/availability` | `@Workflow ReservationWorkflow` | records reservation; calls audit handler (no timer / release-on-expiry) | `inventory.events` | `orders.events` |
| notification | `POST /notifications`, `GET /notifications/by-user/{userId}` | `@Service NotificationService` | stores + emits event; calls audit handler | `notifications.events` | `orders.events`, `payments.events` |
| audit | `POST /audit/events`, `GET /audit/by-aggregate/{id}` | `@Service AuditQueryService` (terminal) | append-only write preserving insertion order | `audit.events` | all `*.events` |

### Internal endpoint addition

Each service exposes `GET /internal/consumed-events` returning its in-memory
consumer-side event list. This is the only knob Plan 1.5's e2e scenarios need
to assert "service X received event Y" without standing up Kafka inspection
tooling. Cheap; can be removed later.

## Data flow

The canary surface is `POST /api/orders` with `x-canary: true` — the spec's
multi-hop saga path (Phase 1 design lines 156-165). The 1.3.a request flow:

1. **order-svc HTTP `POST /api/orders`** — controller writes Order, emits Kafka, then does HTTP fan-out to inventory, payment, notification (saga without compensation per trimmed-C). `CheckoutSaga` workflow is registered with a stub body; **the controller does NOT delegate to it in 1.3.a**. Phase 3 will move the saga steps inside `CheckoutSaga`.
2. **payment, inventory, notification, audit HTTP entry points** — controllers delegate to their service's own Restate handler via Restate Ingress (configured `RestClient`/axios pointed at `RESTATE_INGRESS_URL`). The handler writes to the in-memory store, emits the service's Kafka event, and (for non-terminal services) calls `AuditQueryService.append` via Restate-to-Restate.

**Substrate isolation**: canary pods set `RESTATE_REGISTER_HANDLERS=false` (no handlers registered → Restate runtime routes Ingress calls to stable). Per Phase 1 design line 173, "canary pods can invoke handlers as clients but do not register their own" — the Ingress-delegation pattern is exactly that.

**Live Restate-to-Restate edges in 1.3.a**: payment→audit, inventory→audit, notification→audit. Plus the controller→own-handler hops which transit Restate Ingress (HTTP-to-Restate, x-canary stamped via existing 1.2 lib HTTP interceptor — no new lib primitive needed).

### Sequence diagram — POST /api/orders with x-canary: true

All four non-order services follow the **β pattern** shown for payment below: HTTP controller validates, delegates to its own Restate handler via Ingress (HTTP call to `RESTATE_INGRESS_URL`); handler writes to the service's own in-memory store, emits the service's Kafka event, calls `AuditQueryService.append` via Restate-to-Restate; result returns up the stack. Inventory and notification are collapsed in the diagram for readability.

```mermaid
sequenceDiagram
    autonumber
    participant C as client
    participant O as order-svc (Node)
    participant I as inventory-svc (Java)
    participant P as payment-svc (Java)
    participant N as notification-svc (Node)
    participant A as audit-svc (Java)
    participant R as Restate runtime
    participant K as Kafka

    C->>O: POST /api/orders [x-canary: true]
    Note over O: xCanaryMiddleware → ALS<br/>controller writes Order to store
    O->>K: produce orders.events [x-canary]

    O->>I: axios POST /reservations [x-canary]
    Note over I: same β pattern as payment below<br/>(controller → Ingress → ReservationWorkflow handler → audit R-to-R)
    I-->>O: 201

    O->>P: axios POST /charges [x-canary]
    Note over P: filter → ThreadLocal<br/>controller delegates via Ingress
    P->>R: ingress → PaymentVO[orderId].charge(req)<br/>(HTTP, via RestClient; XCanaryRestClientInterceptor stamps x-canary)
    Note over P: PaymentVO handler runs at payment pod:<br/>state.get() empty → set(charge), writes to store
    P->>K: produce payments.events [x-canary]
    P->>R: ctx.serviceClient(AuditQueryService).call(append, …)<br/>(customizer.apply stamps x-canary on InvocationOptions)
    Note over A: AuditQueryService handler runs at audit pod:<br/>writes AuditEvent to store
    A->>K: produce audit.events
    R-->>P: charge
    P-->>O: 201

    O->>N: axios POST /notifications [x-canary]
    Note over N: same β pattern as payment above<br/>(controller → Ingress → NotificationService handler → audit R-to-R)
    N-->>O: 201

    O-->>C: 201 Order
```

**Key**: `P->>R` is the controller→Ingress hop *or* the handler's outbound R-to-R call (both stamp x-canary, via different SDK surfaces); `R->>...` and `Note over <pod>` indicate where the handler logic actually executes.

### Restate-to-Restate code sketch — Java (PaymentVO → AuditQueryService)

```java
// payment-service/src/main/java/com/canary/payment/handler/PaymentVO.java
@VirtualObject
public class PaymentVO {
  private static final StateKey<Charge> STATE = StateKey.of("charge", Charge.class);

  private final ChargeStore store;                      // ConcurrentHashMap-backed bean; HTTP GET reads from here
  private final XCanaryRestateClientCustomizer canary;
  public PaymentVO(ChargeStore store, XCanaryRestateClientCustomizer canary) {
    this.store = store; this.canary = canary;
  }

  @Handler
  public Charge charge(ObjectContext ctx, ChargeRequest req) {
    // Idempotency: same orderId → same VO instance → same Restate state.
    Optional<Charge> existing = ctx.get(STATE);
    if (existing.isPresent()) return existing.get();

    Charge charge = new Charge(UUID.randomUUID().toString(),
                               req.orderId(), req.amount(), "succeeded");
    ctx.set(STATE, charge);                             // Restate state — idempotency mechanism
    store.put(charge.id(), charge);                     // ConcurrentHashMap — readable via HTTP GET /charges/{id}

    // Customizer reads XCanaryContext.isCanary() and stamps x-canary on opts.
    InvocationOptions opts = canary.apply(InvocationOptions.builder());
    ctx.serviceClient(AuditQueryService.class)
       .call(AuditQueryService::append,
             new AuditEvent("payment", charge.id(), "charged", req.orderId()),
             opts);

    return charge;
  }
}
```

Each service has an in-memory store (`ConcurrentHashMap` Java / module-level
`Map` Node) backing its HTTP GET endpoints; Restate handlers write to this
store. Payment's `@VirtualObject` additionally uses Restate state (`StateKey<Charge>`)
for idempotency-by-orderId — that's the SDK pattern the trimmed-C agreement
buys for free. Inventory's `@Workflow` and the `@Service` handlers
(notification, audit, order's stub) don't have keyed-state idempotency in
1.3.a; they only write to the in-memory store.

Restate handlers aren't Spring components by default. They're registered via
`RestateHttpEndpointBuilder` in a `@Configuration` class that builds them with
Spring-injected dependencies (the store + customizer).
`@ConditionalOnProperty(name="app.restate.register-handlers", havingValue="true",
matchIfMissing=true)` gates the entire Restate endpoint bean.

### Restate-to-Restate code sketch — Node (NotificationService → AuditQueryService)

```typescript
// services/notification-service/src/handlers/notification-handler.ts
import * as restate from "@restatedev/restate-sdk";
import { applyXCanaryToRestateOptions } from "@canary/lib-node";
import { auditQueryServiceDef } from "@canary/restate-defs-node";
import { notificationStore } from "../store.js";   // module-level Map, readable via HTTP GET

export const notificationService = restate.service({
  name: "NotificationService",
  handlers: {
    notify: async (ctx: restate.Context, req: NotifyRequest) => {
      const notification = { id: crypto.randomUUID(), userId: req.userId, status: "sent" };
      notificationStore.set(notification.id, notification);  // readable via HTTP GET /notifications/by-user/{userId}

      // Per-call options helper attaches x-canary metadata when ALS context is canary.
      await ctx.serviceClient(auditQueryServiceDef).append(
        { aggregate: "notification", id: notification.id, action: "sent" },
        applyXCanaryToRestateOptions({}),
      );

      return notification;
    },
  },
});
```

Same shape; the per-call options helper is the propagation surface in both languages.

## Cross-service Restate type sharing

Restate handlers expose a typed surface that callers reference. Two new shared
modules carry these types:

- **`platform/restate-defs-java`** (new Gradle subproject) — Java interfaces + DTOs for `AuditQueryService`, `PaymentVO`, `ReservationWorkflow`, `NotificationService`, `CheckoutSaga`. No Spring deps; just types and Restate annotation imports. Each service implements its own interface from this module; callers depend on it for cross-service Restate calls.
- **`platform/restate-defs-node`** (new pnpm workspace package) — TS service definitions (`auditQueryServiceDef`, `paymentVODef`, etc.) + DTO types. Same pattern.

Restate Java SDK 2.7's annotation processor generates client classes from the
interfaces in `restate-defs-java`. Restate Node SDK 1.14 uses the bare TS defs
for typed `serviceClient<T>(def)` calls.

**x-canary on the controller→Ingress hop**: controllers configure their `RestClient` (Java) or axios (Node) instance against `RESTATE_INGRESS_URL`. The 1.2 HTTP interceptors stamp `x-canary` transparently — no new lib primitive needed. Restate's Ingress passes through HTTP request headers as invocation metadata, so the header arrives at the handler and propagates further via `XCanaryRestateClientCustomizer` / `applyXCanaryToRestateOptions` on outbound Restate-to-Restate calls.

## Configuration

### Java services (Spring Boot conventions)

`services/<svc>/src/main/resources/application.yml` per service, exposed as
`@ConfigurationProperties("app")`:

```yaml
server: { port: 8081 }     # payment; 8082 inventory, 8083 audit
spring:
  application: { name: payment-service }
  kafka: { bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092} }
app:
  kafka:
    consumers: { enabled: ${KAFKA_CONSUMERS_ENABLED:true} }       # canary overlay (1.3.b) sets false
  restate:
    register-handlers: ${RESTATE_REGISTER_HANDLERS:true}            # canary overlay (1.3.b) sets false
    ingress: { url: ${RESTATE_INGRESS_URL:http://localhost:9070} }  # HTTP controller → own handler
  audit: { url: ${AUDIT_URL:http://localhost:8083} }                # only payment, inventory, notification declare this
```

URLs vary per service: payment/inventory declare `audit.url` only; **audit-svc declares no service URLs (terminal)**. Order-svc (Node) declares 3 outbound URLs (`INVENTORY_URL`, `PAYMENT_URL`, `NOTIFICATION_URL`). Standard Spring env-override applies via the `${ENV_VAR:default}` placeholders above.

**Gating points (Java):**

- `@KafkaListener` bean class → `@ConditionalOnProperty(name="app.kafka.consumers.enabled", havingValue="true", matchIfMissing=true)`. When false, the listener bean is not registered → consumer never subscribes.
- Restate endpoint `@Bean` → same `@ConditionalOnProperty` on `app.restate.register-handlers`. When false, no Restate HTTP listener starts → Restate runtime can't reach the handlers.

### Node services (`process.env` at boot in `index.ts`)

```typescript
const KAFKA_CONSUMERS_ENABLED   = process.env.KAFKA_CONSUMERS_ENABLED   !== "false";
const RESTATE_REGISTER_HANDLERS = process.env.RESTATE_REGISTER_HANDLERS !== "false";
const KAFKA_BOOTSTRAP_SERVERS   = (process.env.KAFKA_BOOTSTRAP_SERVERS ?? "localhost:9092").split(",");
const RESTATE_INGRESS_URL       = process.env.RESTATE_INGRESS_URL ?? "http://localhost:9070";
// notification-svc additionally declares AUDIT_URL; order-svc declares INVENTORY_URL + PAYMENT_URL + NOTIFICATION_URL.

if (KAFKA_CONSUMERS_ENABLED)   { await consumer.subscribe({...}); await consumer.run({...}); }
if (RESTATE_REGISTER_HANDLERS) { restate.endpoint().bind(notificationService).http2Listener(9085); }
```

`.env.example` per Node service is committed (not loaded by code). Env-var names are unified across Java and Node so 1.3.b can apply the same env block to every container.

### Local dev port allocation

So all 5 services can run side-by-side on one laptop. (1.3.b k8s pods all bind 8080/9080 inside their containers.)

| Service | HTTP | Restate handler |
|---|---|---|
| payment-svc | 8081 | 9081 |
| inventory-svc | 8082 | 9082 |
| audit-svc | 8083 | 9083 |
| order-svc | 3001 | 9084 |
| notification-svc | 3002 | 9085 |

## Project structure additions

```
settings.gradle.kts:
  include("platform:restate-defs-java")
  include("services:payment-service")
  include("services:inventory-service")
  include("services:audit-service")

pnpm-workspace.yaml:
  - "platform/restate-defs-node"
  - "services/order-service"
  - "services/notification-service"

gradle/libs.versions.toml:
  + restate-sdk-http-vertx  = { module = "dev.restate:sdk-http-vertx",  version.ref = "restateSdk" }
```

Each Java service `build.gradle.kts` applies the `org.springframework.boot`
plugin, depends on `:platform:lib-java`, `spring-boot-starter-web`,
`spring-kafka`, the Restate SDK + `sdk-http-vertx` runtime, and the standard
test deps. Each Node service `package.json` mirrors lib-node's pattern (vitest,
supertest, axios, kafkajs, `@restatedev/restate-sdk`, plus `@canary/lib-node`
workspace dep).

## Build / make integration

```
build-services:   # new — compile all 5 service binaries
    ./gradlew :services:payment-service:bootJar :services:inventory-service:bootJar :services:audit-service:bootJar
    pnpm --filter @canary/order-service build
    pnpm --filter @canary/notification-service build

verify:           # already exists from 1.2 — picks up new tests automatically
                  # once services are registered as Gradle subprojects + pnpm packages.
                  # ./gradlew test runs all Java tests including services;
                  # pnpm -r runs all Node workspace tests.
```

No new test runner config — `make verify` continues to be the single-command CI gate.

## Testing strategy

**Pure unit tests, all I/O mocked.** No EmbeddedKafka, no testcontainers, no live
Restate runtime. lib-java / lib-node primitives are *real* in tests (not
mocked) — service-level tests are how we prove x-canary actually propagates
through the service when requests carry the header.

### Java services — patterns

| Concern | Test type | What's real / mocked |
|---|---|---|
| HTTP controller | `@WebMvcTest(XController.class)` + `MockMvc` | real: filter, controller; mocked: store, `KafkaTemplate`, Restate-side beans |
| Restate handler logic (e.g. `PaymentVO`) | Plain unit test, no Spring context | real: handler, `XCanaryRestateClientCustomizer`; mocked: `ObjectContext` (verify `ctx.set(STATE, …)` and the `serviceClient(…).call(…)` chain) |
| Outbound HTTP propagation | `@SpringBootTest` with `MockRestServiceServer` (or a Mock `RestClient` request executor) | real: `XCanaryRestClientInterceptor` (auto-wired); assert outbound carries `x-canary: true` when `XCanaryContext.isCanary()` |
| Outbound Kafka propagation | Plain unit test on producer config | assert `XCanaryKafkaProducerInterceptor` is present on `interceptor.classes`; round-trip a record through it |
| Consumer gating | `ApplicationContextRunner` toggling `app.kafka.consumers.enabled` | assert `@KafkaListener` bean is present / absent |
| Restate gating | Same pattern toggling `app.restate.register-handlers` | assert Restate endpoint bean is present / absent |
| Application boot | `@SpringBootTest(webEnvironment=NONE)` against `application-test.yml` | smoke — context starts with both flags=true |

`application-test.yml` per service overrides production defaults to keep tests fast (bootstrap servers = `localhost:0`; outbound URLs = `http://example.invalid`).

### Node services — patterns

To make boot unit-testable, structure each Node service's `index.ts` as a thin
entry-point calling exported `setupHttp()`, `setupKafka()`, `setupRestate()`
functions that each accept their dependencies as args. Same pattern as the
lib-node helpers shipped in 1.2.

| Concern | Test type | What's real / mocked |
|---|---|---|
| HTTP routes | vitest + supertest against the Express `app` (not `app.listen()`) | real: `xCanaryMiddleware`, route handler; mocked: KafkaJS producer + Restate client passed as deps |
| Restate handler logic | plain vitest with a mocked Restate `Context` | real: handler, `applyXCanaryToRestateOptions`; mocked: `ctx.serviceClient(…).method(…)` returns a stub |
| Outbound HTTP propagation (e.g. order-svc → payment-svc) | vitest with `axios` adapter mocked | real: `attachXCanaryAxiosInterceptor`; assert captured outbound config has `x-canary` when ALS context is canary |
| Outbound Kafka propagation | vitest, `producer.send` mocked | assert record passed to `send()` was wrapped via `stampXCanaryOnProducerRecord` |
| Consumer gating | unit test on `setupKafka({ consumersEnabled: false })` | assert `consumer.run` not invoked; `consumer.subscribe` not invoked |
| Restate gating | unit test on `setupRestate({ registerHandlers: false })` | assert `endpoint().http2Listener(...)` not invoked |

### Test counts (ballpark)

| Service | Tests |
|---|---|
| payment-svc (Java) | ~10 |
| inventory-svc (Java) | ~10 |
| audit-svc (Java, no outbound HTTP / Restate-to-Restate; still has Kafka producer) | ~8 |
| order-svc (Node, saga fan-out) | ~12 |
| notification-svc (Node) | ~8 |
| **Total new in 1.3.a** | **~48** |

After 1.3.a: ~84 total in `make verify` including the ~36 from 1.2.

If mocking the Restate Ingress wiring on a given controller test becomes onerous (setup overhead exceeds the value of the test), skip those specific cases — Restate-handler logic is independently exercised by the handler-level unit tests, and the `XCanaryRestClientInterceptor` / `attachXCanaryAxiosInterceptor` are already covered by lib-{java,node}'s 1.2 test suite.

## Error handling

Thin throughout 1.3.a:

- **HTTP** — basic shape validation (missing required fields → 400); id-not-in-store → 404; everything else propagates Spring/Express defaults (500). No retry, no circuit breakers, no fallback.
- **HTTP outbound** (`RestClient` / axios) — exceptions bubble to the calling endpoint as 5xx. Order-svc's saga fails fast on the first downstream failure (no compensation per trimmed-C).
- **Kafka producer** — fire-and-forget. Broker unreachable → exception bubbles → 5xx from the caller. No async-confirmation handling.
- **Kafka consumer** — disabled entirely in 1.3.a unit tests. In a real run, default error handler skips poisoned messages (Spring Kafka's `DefaultErrorHandler`; KafkaJS retries-then-skips).
- **Restate handler** — exceptions bubble to the SDK; SDK's automatic retry applies. Phase 3 will add explicit terminal-error handling.
- **Canary feature flags** — invalid/missing values default to `true` (stable behavior). Hardcoded `matchIfMissing=true` (Java) and `!== "false"` (Node) means accidentally unset env vars never flip a stable pod into canary-isolation mode.
