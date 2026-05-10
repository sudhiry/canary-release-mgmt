# Canary Release Management — Phase 3.a Design (Restate substrate completion)

**Status:** Approved (awaiting user review)
**Date:** 2026-05-10
**Phase:** Phase 3, sub-plan a of (a + b)
**Umbrella spec:** `docs/superpowers/specs/2026-05-08-canary-release-phase-1-design.md`
**Predecessors:** Phase 1 (HTTP canary), Phase 2.a + 2.b (Kafka canary), all merged.
**Phase 3.b (successor):** Restate canary handler versioning + per-invocation deployment selection — separate spec.

## Project context

Phase 1.3.a deliberately left three Restate-substrate concerns as stubs/deferrals to keep that sub-plan focused on HTTP canary mechanics:

- `CheckoutSaga.run` (Node) registered as a stub returning `"stub-completed"`. The actual order saga runs as in-controller HTTP fan-out in `services/order-service/src/saga.ts` ([phase-1-3-a-services-design.md:81](2026-05-08-canary-release-phase-1-3-a-services-design.md), [services/order-service/src/saga.ts:24](../../../services/order-service/src/saga.ts)).
- `ReservationWorkflow.run` (Java) writes a `reserved` row and returns immediately. No timer, no `confirm`/`release` lifecycle, no auto-expire ([services/inventory-service/src/main/java/com/canary/inventory/handler/ReservationWorkflowImpl.java:40](../../../services/inventory-service/src/main/java/com/canary/inventory/handler/ReservationWorkflowImpl.java)).
- Restate handler error handling uses `RuntimeException` uniformly. No `TerminalException` distinction between retryable infra failures and non-retryable business/validation errors.

Phase 3.a closes those gaps. After 3.a, Restate is the durable orchestrator for the order saga, every Restate primitive (`@Workflow`, `@VirtualObject`, `@Service`) is exercised meaningfully, and 3.b can build canary handler versioning on top of a complete substrate.

## Locked decisions (from brainstorming)

The following are settled before this design and not revisited:

- **Compensation strategy:** full saga reversal — explicit `releaseReservation` and `refund` compensation handlers, called by the saga in reverse order on each step's failure. Not best-effort dead-letter; not relying on the auto-expire timer for reservation rollback.
- **Compensation symmetry on notify failure:** refund-only with reservation left `confirmed` (partial reversal). The workflow has already terminated via `confirm` at step 2c; there is no parked awakeable to release. This matches real-world saga semantics where committing inventory and reversing it are distinct operations.
- **Reservation lifecycle:** durable workflow with awakeable + 120s timer. `run()` writes `reserved`, then awaits the first of `awakeable.resolve("confirm")`, `awakeable.resolve("release")`, or the timer; transitions status accordingly. `confirm` and `release` are shared workflow handlers that resolve the awakeable. Timer expiry → status `expired` (auto-release; defense in depth for saga crashes between reserve and confirm).
- **HTTP entry path:** `POST /api/orders` switches to invoke `CheckoutSaga.run` via Restate Ingress. `services/order-service/src/saga.ts` is deleted. Restate becomes the only saga codepath.
- **VirtualObject demonstration depth:** `PaymentVO` gains a second handler (`refund`) — multiple handlers on one keyed entity, state lifecycle (`succeeded` → `refunded`), idempotent re-entry. `ReservationWorkflow` stays a `@Workflow` (durable awakeable + timer is the textbook workflow shape; converting it to a VO would lose that pattern).
- **Reservation timer default:** 120 seconds (long enough for normal saga payment + notify steps; short enough to test in opt-in e2e in <3 min).
- **Restate primitive coverage after 3.a:**
  - `@Workflow` = `ReservationWorkflow` (durable wait with timer) + `CheckoutSaga` (saga orchestrator)
  - `@VirtualObject` = `PaymentVO` (multi-handler keyed entity with state lifecycle)
  - `@Service` = `NotificationService`, `AuditQueryService` (stateless RPC)

## Goals

1. Replace the in-controller HTTP fan-out (`saga.ts`) with a real Restate workflow that orchestrates the order saga via R-to-R calls and compensates on failure.
2. Add a durable lifecycle to `ReservationWorkflow`: `reserved` → (`confirmed` | `released` | `expired`) via awakeable + timer.
3. Add a second handler (`refund`) to `PaymentVO` to demonstrate multi-handler VirtualObject patterns.
4. Distinguish `TerminalException` (don't retry — validation, business rules) from regular exceptions (retry per Restate defaults — transient infra) across all Restate handlers.
5. Ship 5 new e2e acceptance scenarios (R1–R5) covering happy path, payment compensation, notify compensation, timer expiry, refund idempotency.

## Non-goals (Phase 3.a)

- Canary handler versioning — Phase 3.b.
- Lifting `RESTATE_REGISTER_HANDLERS=false` on canary pods — Phase 3.b.
- Per-invocation deployment selection — Phase 3.b.
- Production-grade circuit breaking / rate limiting on R-to-R calls — Phase 5.
- Schema evolution on the new Kafka events emitted by the lifecycle/refund handlers — Phase 2.c (deferred).
- Conversion of `Reservation` to a `@VirtualObject` (workflow shape preserved on purpose).
- Replacing the in-memory `ReservationStore`/`ChargeStore` with persistent storage — out of project scope.

## Architecture

Restate becomes the order saga's orchestrator. The HTTP entry path becomes a thin Ingress proxy. Three handler primitives are exercised meaningfully and distinctly.

### Component map

| Layer | Path | Action |
|---|---|---|
| Restate type-defs (Java) | `platform/restate-defs-java/.../payment/PaymentVO.java` | Add `@Handler refund(ChargeRequest)` abstract method |
| Restate type-defs (Java) | `platform/restate-defs-java/.../inventory/ReservationWorkflow.java` | Add `@Shared` handlers `confirm()` and `release()` (workflow shared-handlers operate on the same workflow key as `run`) |
| Restate type-defs (Node) | `platform/restate-defs-node/src/index.ts` (or per-domain files) | Mirror the Java additions in the Node SDK type-defs (`paymentVODef`, `reservationWorkflowDef`) |
| Java VO impl | `services/payment-service/.../handler/PaymentVOImpl.java` | Implement `refund` (state-lifecycle, idempotent, emits Kafka, audits) |
| Java workflow impl | `services/inventory-service/.../handler/ReservationWorkflowImpl.java` | Rewrite `run()` as awakeable+timer lifecycle; implement `confirm` and `release` shared handlers |
| Node saga impl | `services/order-service/src/restate.ts` | Replace stub `checkoutSagaRunHandler` with real saga (R-to-R calls + compensation) |
| Node controller | `services/order-service/src/http.ts` | Replace `runSaga(...)` call (line 71) with `ingressClient.post('/CheckoutSaga/' + orderId + '/run', body)` |
| Node app wiring | `services/order-service/src/index.ts` | Construct `ingressClient: AxiosInstance` against `RESTATE_INGRESS_URL`; drop the per-service axios clients (no longer needed) |
| Node saga (delete) | `services/order-service/src/saga.ts` | **Deleted** |
| Node saga test (delete) | `services/order-service/src/__tests__/saga.test.ts` | **Deleted** |
| Java tests | `services/payment-service/src/test/java/.../PaymentVOImplTest.java` | Add 3 tests covering refund + state-lifecycle |
| Java tests | `services/inventory-service/src/test/java/.../ReservationWorkflowImplTest.java` | Add 4 tests covering awakeable + confirm + release + timer |
| Node tests | `services/order-service/src/__tests__/restate.test.ts` | Replace stub tests with happy-path + 2 compensation paths + x-canary propagation |
| Node tests | `services/order-service/src/__tests__/http.test.ts` | Replace `runSaga` mock with `ingressClient.post` mock |
| E2E (new) | `tests/e2e/r1-r5-restate-saga.test.ts` | 5 new scenarios covering saga + compensation + timer + refund idempotency |

### Restate primitive coverage map (after 3.a)

| Primitive | Handler(s) | What it demonstrates |
|---|---|---|
| `@Workflow` (Node) | `CheckoutSaga.run` | Saga orchestration: 3 R-to-R calls + reverse-order compensation on failure |
| `@Workflow` (Java) | `ReservationWorkflow.run` + shared `confirm` + shared `release` | Durable awakeable + 120s timer + lifecycle states (`reserved` → `confirmed`/`released`/`expired`) |
| `@VirtualObject` (Java) | `PaymentVO.charge` + `refund` | Per-key serialization across multiple handlers + state lifecycle (`succeeded` → `refunded`) + idempotent re-entry |
| `@Service` (Node) | `NotificationService.notify`, `AuditQueryService.append` | Stateless RPC handlers (no change in 3.a) |

## Data flow

### Happy path

```
1. POST /api/orders → order-service controller
   → ingressClient.post(RESTATE_INGRESS_URL + '/CheckoutSaga/' + orderId + '/run', body)
   → x-canary stamped via existing axios interceptor
   → Restate Ingress → CheckoutSaga.run handler

2. Inside CheckoutSaga.run:
   a. ctx.workflowClient(reservationWorkflowDef, orderId).run({sku, quantity, orderId})
      → ReservationWorkflow.run writes 'reserved', emits inventory.events,
        audits, starts awakeable + 120s timer, parks
   b. ctx.objectClient(paymentVODef, orderId).charge({orderId, amount})
      → PaymentVO.charge writes CHARGE_STATE, emits payments.events, audits, returns
   c. ctx.workflowClient(reservationWorkflowDef, orderId).confirm()
      → ReservationWorkflow.confirm resolves awakeable with "confirm"
      → run's await unblocks, status flips 'reserved' → 'confirmed',
        emits inventory.events confirmation, audits
   d. ctx.serviceClient(notificationServiceDef).notify({userId, message, orderId})
      → NotificationService.notify writes Notification, emits notifications.events,
        audits, returns
   e. return Order with status 'completed'

3. Order controller receives the Order back from Ingress, returns 201.
```

### Compensation paths

**Payment fails (TerminalException from `PaymentVO.charge`):**

```
1 + 2a succeed (reservation written, awakeable parked)
2b throws TerminalException
saga catches → ctx.workflowClient(reservationWorkflowDef, orderId).release()
  → ReservationWorkflow.release resolves awakeable with "release"
  → run's await unblocks, status 'reserved' → 'released',
    emits inventory.events release, audits
saga returns Order with status 'failed', reason 'payment-rejected'
```

**Notify fails (TerminalException from `NotificationService.notify`):**

```
1 + 2a + 2b + 2c succeed (reserved + confirmed + charged)
2d throws TerminalException
saga catches → ctx.objectClient(paymentVODef, orderId).refund({orderId, amount})
  → PaymentVO.refund flips CHARGE_STATE.status 'succeeded' → 'refunded',
    emits payments.events refund, audits
saga returns Order with status 'failed', reason 'notification-rejected'
```

(Design note: the reservation was already `confirmed` at step 2c — the workflow has terminated and there is no parked awakeable to `release`. Compensation is **partial-by-design** here: refund the payment; leave the reservation confirmed. The Kafka observer sees `inventory.events confirmed` + `payments.events refunded` and can infer the partial-reversal end state. Calling `release()` after `confirm()` is not modeled by the workflow — there is no "un-confirm" operation, which matches real-world saga semantics where committing inventory and reversing it are distinct operations.)

### Timer expiry (saga crashes between 2a and 2c)

```
2a succeeds, awakeable parked
saga crashes/times out before reaching 2c
After 120s, ctx.timer() resolves first
ReservationWorkflow.run flips status 'reserved' → 'expired',
  emits inventory.events expiry, audits
Inventory is auto-released — defense in depth.
```

## Error-handling matrix

| Scenario | Response | Why |
|---|---|---|
| Validation failure (missing sku/userId/amount) | `TerminalException("invalid request: <field>")` | Retry won't help |
| `PaymentVO.charge` after `refund` | `TerminalException("order already refunded")` | Business rule |
| `PaymentVO.refund` on uncharged order | `TerminalException("no charge to refund for orderId=...")` | Business rule |
| `PaymentVO.refund` on already-refunded order | Return idempotently (no exception) | Idempotent re-entry is correct behavior |
| `ReservationWorkflow.confirm` on already-terminated workflow | `TerminalException("reservation not in confirmable state")` | Race with timer expiry |
| `ReservationWorkflow.release` on terminated workflow | `TerminalException("reservation not in releasable state")` | Workflow already terminal (confirmed/released/expired); no parked awakeable. Saga does NOT attempt this call after confirm — see notify-compensation flow |
| Kafka send throws | propagate as `RuntimeException` | Restate retries the step (transient broker hiccup) |
| R-to-R call broker error | propagate as `RuntimeException` | Restate retries the step |
| `ObjectMapper.writeValueAsString` throws | propagate as `RuntimeException` | Should be impossible at runtime; if it happens, retrying probably won't fix but it's not a "user error" either |

## Testing

### Unit tests (mock-based, fast)

**`ReservationWorkflowImplTest`** — extend existing tests:
- `runReservesAndAwaitsAwakeable`
- `confirmResolvesAwakeableAndStatusFlipsToConfirmed`
- `releaseResolvesAwakeableAndStatusFlipsToReleased`
- `timerExpiryFlipsStatusToExpiredEvenIfConfirmCalledLater`
- `confirmAfterTimerExpiryThrowsTerminalException`

**`PaymentVOImplTest`** — extend existing tests:
- `refundFlipsStateToRefundedAndEmitsKafkaEvent`
- `refundIsIdempotentWhenAlreadyRefunded`
- `refundOnUnchargedOrderThrowsTerminalException`
- `chargeAfterRefundThrowsTerminalException`

**`services/order-service/src/__tests__/restate.test.ts`** — replace stub tests:
- `happyPathExecutesAllStepsInOrder`
- `paymentTerminalErrorTriggersReleaseReservation`
- `notifyTerminalErrorTriggersRefundOnly` (reservation stays `confirmed` — partial reversal)
- `xCanaryHeaderPropagatedToAllRtoRCalls`

**Service-level tests:**
- `services/order-service/src/__tests__/saga.test.ts` — **deleted**.
- `services/order-service/src/__tests__/http.test.ts` — replace `runSaga` mock with `ingressClient.post` mock; assert HTTP→Ingress proxy shape.
- `inventory-service` controller tests — unchanged.
- `payment-service` controller tests — unchanged.

### E2E scenarios (new file: `tests/e2e/r1-r5-restate-saga.test.ts`)

| # | Name | What it asserts |
|---|---|---|
| R1 | Saga happy path | All four R-to-R steps fire (verified via consumed-events + service stores), reservation `confirmed`, payment `succeeded`, notification stored |
| R2 | Saga payment compensation | Payment refuses (e.g., `amount: -1` triggers TerminalException) → reservation `released`, payment store has no charge |
| R3 | Saga notify compensation | Notification refuses (e.g., `userId: 'reject-me'`) → payment `refunded` (state lifecycle); reservation stays `confirmed` (partial reversal — see compensation flow in spec) |
| R4 | Reservation timer expiry | Invoke `ReservationWorkflow.run` directly (not via saga); wait 130s; assert status `expired`, `inventory.events` has expiry record |
| R5 | Refund idempotency under retry | Invoke `PaymentVO.refund` twice; assert single `payments.events` refund record (Kafka idempotency proven by VO state-check) |

R1–R5 use the existing K1–K5 port-forward pattern (open subset port-forwards, query `/internal/consumed-events`, check stores via existing helpers).

R4 and R5 are gated behind `RUN_SLOW_E2E=true` because R4's 130s wait is non-trivial wall-time. R1–R3 run in the default opt-in scenario suite.

## Out of scope (explicit)

- Canary handler versioning (Phase 3.b).
- `RESTATE_REGISTER_HANDLERS=false` lift on canary pods (Phase 3.b).
- Per-invocation deployment selection (Phase 3.b).
- Schema evolution on the new Kafka events (Phase 2.c, deferred).
- Production observability polish on saga steps (Phase 5).
- Persistent stores backing Reservation/Charge — out of project scope.
- `Reservation` as VirtualObject — workflow shape preserved on purpose.

## Migration / rollout

- Atomic single PR. The Restate handler ABI changes (new abstract methods on `PaymentVO`, new shared handlers on `ReservationWorkflow`) cannot be deployed as a lib-only update without service updates — both must merge together.
- No existing-state migration. The Reservation/Charge stores are in-memory; pod restart resets them.
- Helm + canary-overlay: no changes (Phase 3.b owns the canary; 3.a leaves `RESTATE_REGISTER_HANDLERS=false` on canary pods unchanged).
- Cluster verification: opt-in. Run R1–R3 in the default scenario suite (~5 min). Run R4–R5 separately under `RUN_SLOW_E2E=true` if changing timer logic.

## Risks

- **Awakeable parking cost.** The reservation workflow parks for up to 120s holding journal state. With dev workloads invisible; under production load, parked workflows consume Restate journal bytes proportional to (request rate × 120s). Flagged for awareness; not a 3.a blocker.
- **Confirm-vs-timer race.** Restate's first-resolved semantics make this deterministic, but the "confirm-after-expiry → TerminalException" rule is the contract that handles legitimate races. R4's test exercises only the timer-wins-uncontended case; the contested race is unit-test-only.
- **R4 e2e wall-time.** 130s is significant. Acceptable for opt-in cluster verification; gated behind `RUN_SLOW_E2E=true`. If the timer constant is ever changed, R4's wait must be re-tuned.
- **`saga.ts` deletion may surprise.** Mitigated by `docs/canary-mechanics.md` update describing the new flow; old in-controller pattern referenced only in `docs/history.md`.
- **`HttpDeps` interface change in order-service.** The `clients: SagaClients` field is removed. Any remaining test that constructs `setupHttp(...)` must be updated. The plan should call this out explicitly.
