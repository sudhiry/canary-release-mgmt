# Canary Release Phase 3.b — Canary Handler Versioning (Restate) — Design

**Status:** approved (brainstorming → spec)
**Date:** 2026-05-11
**Predecessors:** Phase 3.a (Restate substrate completion, merged 2652419)

## Goal

Lift the `RESTATE_REGISTER_HANDLERS=false` constraint on canary pods and demonstrate header-routed subset isolation through Restate, mirroring Phase 1 (HTTP) and Phase 2 (Kafka). A request flagged with `x-canary: true` traverses canary versions of every Restate handler in the saga end-to-end; an unflagged request traverses stable versions. Both subsets coexist as long-lived steady states (95/5 split is supported).

## Non-goals

- **Restate's native rolling-versioning model** ("latest-wins" with in-flight drainage). We deliberately route *around* it. Documented as alternative α below.
- **Schema migration of Restate state.** Behavioral tweaks are response-shape additions only; persisted state in `ReservationStore` and `PaymentVO` state remains byte-compatible with Phase 3.a.
- **Production-grade canary teardown automation.** Operator runbook is documented; tooling is not built.
- **Cross-cluster Restate.** Single-cluster Restate only, as in Phase 3.a.

## Alternatives considered

### α — Embrace Restate's native versioning (rejected)

Each pod registers under the unsuffixed service name (`CheckoutSaga`, etc.). Restate's registry holds *one* `deployment_id` per service ("the latest revision"); the most recent registration wins, and new invocations route there. In-flight invocations drain on their original deployment.

**Why rejected:**
- Breaks Phase 1+2's `x-canary` header-routing semantics. The header has no effect on Restate dispatch; only the order of registration determines routing.
- No steady 95/5 split is possible — Restate routes 100% of new invocations to the latest deployment.
- Multi-replica registration races: every pod restart re-registers and can flip the "latest" pointer.
- Demo value is "rolling deploy with in-flight safety," not "subset isolation."

### β — Header-based routing on top of Restate (chosen)

Stable and canary register as *distinct* services (e.g., `CheckoutSagaStable` vs `CheckoutSagaCanary`). The order-service HTTP controller reads `x-canary` and posts to the matching service name via Restate Ingress. Restate sees them as unrelated services with independent `latest` deployments — no race, no drain.

**Trade-offs accepted:**
- Per-canary-handler duplication of service-name registration.
- Restate UI shows 4 logical services × 2 variants = 8 entries.
- In-flight invocations on a torn-down canary deployment retry indefinitely; operator must intervene to drain (no automatic drainage like α). Documented runbook.
- **Restate's documented `restate invocations pause` / `restate invocations resume --deployment <id>` recovery is unavailable in β.** Pause-resume requires the resume target to expose the same service name as the original deployment; β registers `*Canary` and `*Stable` as distinct services, so a `CheckoutSagaCanary` invocation cannot be resumed onto a deployment that hosts only `CheckoutSagaStable`. β operators must either drain naturally or `restate invocations cancel <id>` — see Operational runbook.

**Why chosen:** consistency with Phase 1+2's `x-canary` semantics is the project's central thesis. β preserves end-to-end header routing through Restate at the cost of name-duplication and forfeits the pause-resume recovery primitive.

## Architecture

### Restate registry after Phase 3.b

| Service                         | Deployment URL                                                        |
|---------------------------------|-----------------------------------------------------------------------|
| `CheckoutSagaStable`            | `http://order-service-stable.services.svc.cluster.local:9084`         |
| `CheckoutSagaCanary`            | `http://order-service-canary.services.svc.cluster.local:9084`         |
| `ReservationWorkflowStable`     | `http://inventory-service-stable.services.svc.cluster.local:9082`     |
| `ReservationWorkflowCanary`     | `http://inventory-service-canary.services.svc.cluster.local:9082`     |
| `PaymentVOStable`               | `http://payment-service-stable.services.svc.cluster.local:9081`       |
| `PaymentVOCanary`               | `http://payment-service-canary.services.svc.cluster.local:9081`       |
| `NotificationServiceStable`     | `http://notification-service-stable.services.svc.cluster.local:9085`  |
| `NotificationServiceCanary`     | `http://notification-service-canary.services.svc.cluster.local:9085`  |

### Request-path summary (canary)

```
client (x-canary: true)
  → Istio routes to order-service-canary pod
  → http.ts reads x-canary → posts to /CheckoutSagaCanary/<orderId>/run
  → Restate dispatches to order-service-canary:9084 (registered for *Canary)
  → canary saga runs, with downstream clients pre-bound to *Canary defs:
      - workflowSendClient(reservationWorkflowCanaryDef).run(...)   → inventory-service-canary
      - objectClient(paymentVOCanaryDef).charge(...)                → payment-service-canary
      - workflowClient(reservationWorkflowCanaryDef).confirm(...)   → inventory-service-canary
      - serviceClient(notificationServiceCanaryDef).notify(...)     → notification-service-canary
  → returns Order { ..., auditTrail: ["saga@canary","reservation@canary","payment@canary","notification@canary"] }
```

Stable mirror: `x-canary` absent → `/CheckoutSagaStable/...` → all `*Stable` deployments.

### Subset isolation guarantees

Three independent layers all carry the same variant for any given invocation:
1. **Registration:** each pod reads `process.env.VERSION` (Node) / `app.version` Spring property (Java) at boot and registers exactly one variant.
2. **In-saga client construction:** each pod's saga module reads the same `MY_VARIANT` and binds all downstream clients to matching-variant defs.
3. **K8s endpoint selection:** the per-subset Service (`<name>-<version>`) selector includes the `version` label, so Restate's dispatch URL only resolves to pods of that variant.

A `CheckoutSagaCanary` invocation cannot reach a stable handler by any path.

### `x-canary` header retention

Service-name selection now drives Restate routing, but the `x-canary` header continues to flow via `applyXCanaryToRestateOptions` (Node) and `XCanaryRestateClientCustomizer` (Java). It populates the X-Served-Chain audit header on every hop; it does *not* drive routing. Routing is purely service-name-based.

## Behavioral tweaks per canary handler

Each canary handler differs from its stable counterpart in *one observable behavior*, expressed in a response-payload field. Tests assert these are present.

| Handler                  | Canary tweak                                                                |
|--------------------------|-----------------------------------------------------------------------------|
| `CheckoutSaga`           | Returns `auditTrail: string[]` with `<svc>@canary` entries per hop.         |
| `ReservationWorkflow`    | `Reservation.bufferUnits = 1` (canary) vs `0` (stable). Response-time only; not persisted in `ReservationStore`. |
| `PaymentVO`              | `chargedAmount = (amount * 99) / 100` (canary, integer cents) vs `amount` (stable). Returned in `ChargeResponse`. |
| `NotificationService`    | Appends `[via canary notifier]` to delivered message; returned in `NotifyResult.deliveredMessage`. |

These are independent — none depends on another. All are observable in the saga's response without slow timers.

## Component & file inventory

`+` = create, `~` = modify, `-` = delete.

### Restate definition packages

```
platform/restate-defs-java/src/main/java/com/canary/restate/inventory/
  - ReservationWorkflow.java
  + ReservationWorkflowStable.java                # @Workflow(name="ReservationWorkflowStable")
  + ReservationWorkflowCanary.java                # @Workflow(name="ReservationWorkflowCanary")
  ~ Reservation.java                               # add `int bufferUnits`

platform/restate-defs-java/src/main/java/com/canary/restate/payment/
  - PaymentVO.java
  + PaymentVOStable.java
  + PaymentVOCanary.java
  + ChargeResponse.java                            # new return type for charge()

platform/restate-defs-node/src/index.ts
  ~ replace each unsuffixed Def export with stable+canary pair, constructed via
    a `makeXxxDef(variant)` helper:
       checkoutSagaStableDef / checkoutSagaCanaryDef
       paymentVOStableDef / paymentVOCanaryDef
       reservationWorkflowStableDef / reservationWorkflowCanaryDef
       notificationServiceStableDef / notificationServiceCanaryDef
  ~ Order interface: add `auditTrail: string[]`
  + NotifyResult interface returned by notify()
```

The old unsuffixed names are deleted, not aliased. Phase 3.a hasn't been externally released.

### Java service implementations (inventory + payment)

Same shape for both. Inventory shown:

```
services/inventory-service/src/main/java/com/canary/inventory/handler/
  ~ ReservationWorkflowImpl.java         → renamed to ReservationWorkflowCore.java
  + ReservationWorkflowCore.java          # actual logic; ctor takes `boolean isCanary`;
                                          # injects bufferUnits=isCanary?1:0 into Reservation responses
  + ReservationWorkflowImplStable.java    # extends ReservationWorkflowStable;
                                          #   ctor: super(); core = new Core(false)
                                          #   delegates run/confirm/release to core
  + ReservationWorkflowImplCanary.java    # mirror with isCanary=true

services/inventory-service/src/main/java/com/canary/inventory/config/
  ~ RestateEndpointConfig.java            # variant-conditional bean wiring:
                                          #   @Bean @ConditionalOnProperty(name="app.version", havingValue="stable")
                                          #     reservationWorkflowImplStable(...)
                                          #   @Bean @ConditionalOnProperty(name="app.version", havingValue="canary")
                                          #     reservationWorkflowImplCanary(...)
                                          # Endpoint.builder().bind(handler).build() picks up whichever bean exists.
                                          # Startup self-check: assert app.version matches a registered impl.

services/inventory-service/src/test/java/...
  + ReservationWorkflowCoreTest.java      # parameterized unit tests: isCanary=false / true
  ~ RestateEndpointGatingTest.java        # add cases for app.version=stable, =canary, =unknown
```

Mirror in `payment-service`: `PaymentVOCore.java`, `PaymentVOImpl{Stable,Canary}.java`, equivalent config and test changes.

### Node service implementations (order + notification)

```
services/order-service/src/restate.ts
  ~ Read MY_VARIANT = process.env.VERSION ?? "stable" once at module load.
  ~ Construct exactly one saga def using checkoutSaga<MY_VARIANT>Def.
  ~ All downstream client constructions inside the saga body use matching-variant defs.
  ~ Bind that single saga to the Restate endpoint.

services/order-service/src/http.ts
  ~ POST /api/orders handler reads x-canary from incoming request headers.
  ~ Builds Restate Ingress URL: `/CheckoutSaga${variant}/${orderId}/run` where
    variant = headers["x-canary"] === "true" ? "Canary" : "Stable".
  ~ This is the SOLE place x-canary maps to a service-name decision. Inside the
    saga, the variant is fixed at boot time and never reconsulted.
  ~ Saga response includes auditTrail array; controller passes it through to the client.

services/order-service/src/__tests__/
  + saga-variant-binding.test.ts          # asserts saga module's bound def name
                                          # and downstream client construction match VERSION env
  ~ http.test.ts                          # adds x-canary → variant URL routing assertions

services/notification-service/src/restate.ts
  ~ Same factory pattern. Canary impl appends "[via canary notifier]" to the delivered
    message body; returns NotifyResult { delivered, version, deliveredMessage }.
```

### Helm chart

```
deploy/helm/service-chart/templates/
  ~ service.yaml                  # single file, two Service kinds:
                                  #   - shared <name> Service: gated `if eq version "stable"`
                                  #   - per-subset <name>-<version> Service: always rendered
  ~ restate-register-job.yaml     # registration URL: <name>-<version> instead of <name>
  ~ deployment.yaml               # remove canary's RESTATE_REGISTER_HANDLERS=false override
                                  # (or have canary inherit the stable default of true)

deploy/helm/values/
  ~ <each-service>.yaml           # if any baked-in canary RESTATE_REGISTER_HANDLERS=false
                                  #   exists per-service, remove it
```

**Impact on Phase 1 (HTTP):** zero. Istio's VS/DR target the shared `<name>` Service, which is unchanged. Adding `<name>-<version>` Services alongside doesn't perturb Istio.

**Impact on Phase 2 (Kafka):** zero. Kafka traffic doesn't use K8s Services at all.

### Tests (e2e)

```
tests/e2e/
  ~ r1-r5-restate-saga.test.ts                 # parameterize each scenario across variants;
                                                # assert auditTrail / bufferUnits / chargedAmount /
                                                # NotifyResult.version reflect the requested variant
  + r6-restate-canary-isolation.test.ts        # 10 concurrent flagged+unflagged orders;
                                                # assert no cross-contamination of variant in any response
                                                # (fast; no slow timers)
  + r7-restate-canary-deploy-lifecycle.test.ts # gated on RUN_CANARY_LIFECYCLE_TESTS=true:
                                                # - assert both variant deployments registered
                                                # - assert per-subset K8s Services with correct selectors
                                                # - canary teardown: assert *Canary 404, *Stable still works
```

## Data shapes

### `Reservation` (Java POJO, in restate-defs-java)

```java
public record Reservation(
    String orderId,
    String sku,
    int quantity,
    String state,         // "reserved" | "confirmed" | "released" | "expired"
    int bufferUnits        // NEW: 0 stable, 1 canary; derived at response time
) {}
```

### `ChargeResponse` (Java POJO, new in restate-defs-java)

```java
public record ChargeResponse(
    String orderId,
    long requestedAmount,
    long chargedAmount,    // amount stable, (amount * 99) / 100 canary
    String state           // "succeeded" | "refunded"
) {}
```

### `ReservationWorkflow` abstract handlers

```java
@Workflow(name = "ReservationWorkflowStable")    // or ...Canary
public abstract class ReservationWorkflowStable {
    @Handler  public abstract Reservation run(ReservationRequest req);
    @Shared   public abstract Reservation confirm();    // was: void
    @Shared   public abstract void release();           // unchanged (failure path)
}
```

### `PaymentVO` abstract handlers

```java
@VirtualObject(name = "PaymentVOStable")    // or ...Canary
public abstract class PaymentVOStable {
    @Handler  public abstract ChargeResponse charge(ChargeRequest req);   // was: void
    @Handler  public abstract void refund(ChargeRequest req);             // unchanged
}
```

### `NotifyResult` (TypeScript interface)

```typescript
export interface NotifyResult {
  delivered: boolean;
  version: "stable" | "canary";
  deliveredMessage: string;    // canary appends "[via canary notifier]"
}
```

### `Order` (TypeScript interface)

```typescript
export interface Order {
  id: string;
  userId: string;
  sku: string;
  quantity: number;
  amount: number;
  status: "pending" | "completed" | "failed";
  auditTrail: string[];     // NEW: per-hop "<svc>@<variant>" entries
}
```

### Saga audit-trail assembly

Saga's `MY_VARIANT` is read once at boot. Trail is assembled from saga's own variant + each step's response:

```typescript
const auditTrail = [`saga@${MY_VARIANT}`];

// Step 1: reserve (fire-and-forget; no return; trust by-construction matching variant)
auditTrail.push(`reservation@${MY_VARIANT}`);

// Step 2: charge
const chargeResp = await paymentClient.charge(...);
auditTrail.push(`payment@${chargeResp.chargedAmount === amount ? "stable" : "canary"}`);

// Step 3: confirm (now returns Reservation with bufferUnits)
const reservation = await reservationClient.confirm();
// reservation.bufferUnits already verifies the variant

// Step 4: notify
const notifyResp = await notificationClient.notify(...);
auditTrail.push(`notification@${notifyResp.version}`);
```

Reservation and saga entries trust the by-construction invariant; payment and notification entries are *verified* via response math/labels — falsifiable in tests.

## Error handling & failure modes

### Compensation (unchanged from Phase 3.a)

```
Step 1 (reserve)  fails → return failed (reservation auto-expires via 120s timer)
Step 2 (charge)   fails → call <variant>-reservation.release(); return failed
Step 3 (confirm)  fails → call <variant>-payment.refund();      return failed (timer raced)
Step 4 (notify)   fails → call <variant>-payment.refund() ONLY; reservation stays confirmed
                          (partial reversal, by design)
```

Each compensation goes to the matching-variant downstream automatically — clients are bound at saga construction.

### β-specific failure modes

| ID  | Failure                                                 | Surfacing / mitigation                                                                                                  |
|-----|---------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------|
| F1  | One variant's registration Job fails                    | `helm upgrade --wait` blocks on Job success. Visible via `kubectl get jobs`. R7 asserts both deployments registered.    |
| F2  | Cold canary, request arrives before registration        | Restate Ingress 404 → controller returns 503. Tests gate flagged traffic on canary pods Ready (kubectl wait pattern).   |
| F3  | Canary torn down with in-flight invocations             | Restate retries indefinitely on dead URL. Operator runbook: stop flagged traffic at Istio, drain in-flights via Restate Admin UI, `DELETE /deployments/<canary-id>`, then `helm uninstall`. Documented limitation of β.   |
| F4  | Variant-mismatch within saga (refactor regression)      | `saga-variant-binding.test.ts` static check on bound def names per VERSION env.                                         |
| F5  | Pod label / VERSION env mismatch (config drift)         | Spring + Node startup self-check: assert `process.env.VERSION` (or `app.version`) matches expected variant; fail fast.  |
| F6  | Controller routes flagged request to wrong variant URL  | `http.test.ts` unit test + R1-R6 e2e auditTrail assertions.                                                             |
| F7  | Canary unavailable during traffic — no automatic fallback to stable | Phase 2 (Kafka) handles this via per-message filter + K8s pod-watch (`canaryReady`): stable picks up flagged events when canary is dead. Phase 3.b does **not** replicate that. The order-service controller routes by header alone — when canary deployments are unhealthy, flagged requests POST to `/CheckoutSagaCanary/...` and Restate either 404s (no canary registered) or retries indefinitely (registered but URL unreachable). Failure surfaces as HTTP 502/503 to the client. Mitigation is operational — fix canary or stop sending flagged traffic at Istio. By design, not a bug; the synchronous request path makes failure observable to the client (unlike Phase 2's Kafka black-hole risk), so the elaborate watch-and-takeover machinery isn't load-bearing here. See "Asymmetry with Phase 2" note below.    |

### TerminalException / retryable distinction

Unchanged from Phase 3.a. Both `*Stable` and `*Canary` impls share error semantics through `*Core`.

## Testing

### Layers

| Layer            | Files                                                          | Gating                            |
|------------------|----------------------------------------------------------------|-----------------------------------|
| Java unit (Core) | `ReservationWorkflowCoreTest`, `PaymentVOCoreTest`             | Every commit                      |
| Spring wiring    | `RestateEndpointGatingTest` (extended)                         | Every commit                      |
| Node unit        | `saga-variant-binding.test.ts`, `http.test.ts` (extended)      | Every commit                      |
| E2E (fast)       | `r1-r5-*` (variant-extended), `r6-*-isolation`                 | Every e2e run                     |
| E2E (slow)       | R4-R5 timer paths                                              | `RUN_SLOW_E2E=true`               |
| E2E (cluster)    | `r7-*-deploy-lifecycle`                                        | `RUN_CANARY_LIFECYCLE_TESTS=true` |

### Coverage matrix

| Failure mode | Caught by                                         |
|--------------|---------------------------------------------------|
| F1           | R7 (cluster lifecycle)                            |
| F2           | R7                                                |
| F3           | R7 (teardown step)                                |
| F4           | `saga-variant-binding.test.ts`                    |
| F5           | startup self-check (log-asserted, not e2e)        |
| F6           | `http.test.ts` + R1-R6 auditTrail assertions      |
| F7           | Documented; no test (operational concern, not a code path) |

### Asymmetry with Phase 2's stable-takes-over rule

Phase 2's spec includes "rule #2 — if `x-canary=true` AND canary pod NOT deployed, stable processes (fallback)." Implementation: stable's Kafka listener checks `xCanary && canaryReady` before skipping; `canaryReady` is maintained by a K8s pod-watch on `app=<svc>,version=canary` selector. This is essential because Kafka events that nobody consumes are *silently dropped* — fallback prevents black-hole.

Phase 3.b does **not** implement an analogous fallback. The order-service HTTP controller routes by `x-canary` header alone, with no awareness of canary deployment health. Rationale:

- **Failure shape is different.** A flagged HTTP request that hits a dead canary returns HTTP 502/503 to the client *immediately observable*. The client (or its retry layer) can drop the flag and retry without it. There's no silent loss like Kafka's case.
- **Cost of parity is non-trivial.** A faithful Phase 2 mirror would require: (i) a K8s pod-watch on `app=order-service,version=canary` in order-service-stable; (ii) controller logic that reads both header AND `canaryReady` before choosing `/CheckoutSaga<Variant>/...`; (iii) similar plumbing in any future service that fronts a saga. Possible, but implementation + operational complexity isn't justified by the synchronous-error-path's existing surfacing.
- **Operational mitigation is sufficient for a reference architecture.** Operators detect canary outages via standard pod readiness alarms; flagged traffic is stopped at the Istio VirtualService; canary is fixed or retired via the runbook above. The pod-watch + filter machinery from Phase 2 is over-engineering on top of synchronous semantics.

If automatic fallback becomes a real operational requirement (e.g., in a production fork of this reference architecture), Option B from the original review applies: extend the existing Phase 2 `presenceWatcher` in order-service to publish a `canaryReady` boolean, and have the HTTP controller fall through to `*Stable` when `canaryReady` is false. ~50 lines of code; reuses existing watch infrastructure.

R6 is the headline fast-path subset-isolation assertion. R7 is the headline cluster-verify that proves the operational story (registration coexistence, teardown).

## Operational runbook (canary teardown)

Restate's official versioning docs (https://docs.restate.dev/services/versioning) recommend two recovery mechanisms for retiring a deployment that still has in-flight work:

1. **Drain-then-remove** — `restate deployment describe <id> --extra` to inventory in-flights; `restate deployments remove <id>` only succeeds once drained.
2. **Pause-resume** (highlighted as the *preferred* method for stuck/buggy in-flights) — `restate invocations pause <id>` then `restate invocations resume <id> --deployment <new_id>`, where `<new_id>` is a different deployment of the **same service**.

**β disables pause-resume by construction.** Stable and canary register as distinct services (`*Stable` vs `*Canary`); a `CheckoutSagaCanary` invocation cannot be resumed onto the stable deployment because that deployment doesn't host `CheckoutSagaCanary`. Replay would fail with "service not found." Recovery in β is therefore limited to drain-and-remove, with `restate invocations cancel` as the only escape hatch for stuck work.

### Procedure

1. **Stop new flagged traffic.** Edit the Istio VirtualService route to 100% stable subset, or remove the canary subset rule. New `x-canary: true` requests now hit the stable saga.

2. **Inventory in-flight `*Canary` invocations:**
   ```
   restate deployment describe <canary-deployment-id> --extra
   ```

3. **Drain — choose mode:**

   **(a) Graceful (preferred when latency budget allows).** Let in-flights finish naturally. `ReservationWorkflowCanary.run` parks on a 120s timer, so worst-case wait per saga is ~2 minutes. Watch:
   ```
   watch restate deployment describe <canary-deployment-id> --extra
   ```
   Wait until the in-flight count is 0.

   **(b) Emergency (when canary pods are gone or the deployment is genuinely buggy).** Cancel each in-flight invocation:
   ```
   for id in $(restate invocations list --deployment <canary-deployment-id> --json | jq -r '.[].id'); do
     restate invocations cancel "$id"
   done
   ```
   Cancellation triggers Restate's compensation path *only if the saga catches the cancellation signal* — the current Phase 3.b sagas do not handle cancellation explicitly, so durable side effects (Charges in `succeeded` state, Reservations in `reserved` state) may be left mid-flight. Operators must manually reconcile via the per-service Admin endpoints (`POST /api/orders/<id>/refund`, `POST /api/reservations/<id>/release`, etc.) if applicable. Document this trade-off when running emergency drain.

   *Pause-resume is not an option.* If a future operator runs `restate invocations resume <id> --deployment <stable-deployment-id>`, Restate will refuse — the stable deployment doesn't expose `CheckoutSagaCanary`. This is by design.

4. **Deregister the canary deployment from Restate:**
   ```
   restate deployments remove <canary-deployment-id>
   ```
   (Add `--force` only if step 3 used emergency mode and you've already accepted the partial-state cost.)

5. **Tear down the K8s release:**
   ```
   helm uninstall <canary-release> -n services
   ```

Skipping step 3 leaves Restate retrying dead URLs indefinitely. Skipping step 4 means future canary installs that re-register at the same URL will collide.

## Operational expectations

- `helm upgrade --wait` should be used for both stable and canary releases so that the registration Job's success is verified before traffic is sent.
- Both stable and canary releases set `version` Helm value explicitly. The `templates/service.yaml` defaults `version` to `"stable"` for backward compatibility.
- Restate Server pin remains `1.6.2` per the version-compatibility memory; no server-side change required.
