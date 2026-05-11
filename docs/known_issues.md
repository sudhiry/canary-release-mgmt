# Known Issues — Over-Engineering

Captured 2026-05-11 after a whole-project review. Scope: ~15K LOC across `platform/`, `services/`, `tools/`, `deploy/`, `tests/`, `docs/`. The project is feature-complete through Phase 5; this document lists known structural debt that should be addressed before any new phase begins.

Each item lists: **location**, **what**, **why it's over-engineered**, **fix**, **severity**.

---

## Naming / API clarity

### N1 — `canary_*` metric prefix is misleading

- **Location**: [platform/lib-java/.../CanaryMetrics.java:12-14](../platform/lib-java/src/main/java/com/canary/platform/lib/observability/CanaryMetrics.java), 3 dashboards under [deploy/kind/observability/dashboards/](../deploy/kind/observability/dashboards/), ~6 test files
- **What**: The metrics `canary_request_total`, `canary_request_duration_seconds`, and the classes `CanaryMetrics` / `CanaryHttpSpanFilter` / `CanaryKafkaRecordInterceptor` / `CanaryRestateMeter` are named as if they only emit for canary traffic.
- **Why it's an issue**: They emit for **both** lanes — the `lane` tag resolves to `"stable"` or `"canary"` via [CanaryLaneTag.current()](../platform/lib-java/src/main/java/com/canary/platform/lib/observability/CanaryLaneTag.java). New readers reasonably assume the prefix means "canary-only" and ask "where's my stable observability?" Coverage is correct; naming is wrong.
- **Fix**: Rename to `app_request_total{lane=…}` / `app_request_duration_seconds`. Keep `canary_lane_active` and `canary_shadow_mismatch_total` (those are genuinely canary-rollout-specific). Touches metric constants + 3 dashboard JSONs + ~6 tests.
- **Severity**: Low (cosmetic, but causes confusion every time someone reads the package).

---

## Platform libraries

### P1 — `LaneStateProbe` duplicates kube-state-metrics

- **Location**: [platform/lib-java/.../LaneStateProbe.java](../platform/lib-java/src/main/java/com/canary/platform/lib/observability/LaneStateProbe.java) (92 LOC) + bean wiring in [CanaryMetricsAutoConfiguration.java:45-54](../platform/lib-java/src/main/java/com/canary/platform/lib/observability/CanaryMetricsAutoConfiguration.java)
- **What**: In-process K8s watcher per Java pod that emits `canary_lane_active{lane=stable|canary}`.
- **Why**: kube-state-metrics already exposes `kube_endpoint_address_available{endpoint=~".*-(stable|canary)"}`. Same signal, no fabric8 client embedded in every Java service, no per-pod watch connections.
- **Fix**: Delete `LaneStateProbe`, its bean, its test, and the fabric8 dependency from `platform/lib-java/build.gradle.kts`. Replace the dashboard PromQL with kube-state-metrics equivalents.
- **Severity**: Medium (removes a dependency from every Java service).

### P2 — `TracingAutoConfiguration` MeterFilter is defensive for an impossible bug

- **Location**: [platform/lib-java/.../TracingAutoConfiguration.java](../platform/lib-java/src/main/java/com/canary/platform/lib/observability/TracingAutoConfiguration.java)
- **What**: A `MeterFilter` that denies any non-`canary_*` meter carrying a `lane` tag.
- **Why**: Safety net for a mistake (accidentally setting a global `commonTags("lane", …)`) that nobody has made. The Javadoc itself says "surface and fix the offending `commonTags` call rather than relying on this filter as a long-term safety net."
- **Fix**: Delete the class. If the mistake ever happens, fix it at the source.
- **Severity**: Low.

### P3 — `CanaryRestateMeter` wrapper used by one service in two callsites

- **Location**: [platform/lib-java/.../CanaryRestateMeter.java](../platform/lib-java/src/main/java/com/canary/platform/lib/observability/CanaryRestateMeter.java) — used only by `PaymentVOImpl{Canary,Stable}.java`
- **What**: `measure(name, body)` wrapper + custom `ThrowingSupplier<T>` inner interface for around-handler timing.
- **Why**: Single consumer, two callsites; `ThrowingSupplier` re-invents `java.util.concurrent.Callable`.
- **Fix**: Inline the 8 lines of timing/record into the two payment handlers, OR keep the wrapper but replace `ThrowingSupplier` with `Callable<T>`.
- **Severity**: Low.

### P4 — Test-only static methods exposed as public API

- **Location**: [platform/lib-java/.../XCanaryPresenceWatcher.java:111-126](../platform/lib-java/src/main/java/com/canary/platform/lib/XCanaryPresenceWatcher.java)
- **What**: `computeCanaryReady()` and `isPodReady()` are `static` only so tests can call them directly; never used at runtime.
- **Fix**: Inline logic into `start()`; rewrite tests to drive through the public `isCanaryReady()` API.
- **Severity**: Low.

### P5 — Unused configuration knobs

- **Location**: [platform/lib-java/.../XCanaryAutoConfiguration.java:102](../platform/lib-java/src/main/java/com/canary/platform/lib/autoconfigure/XCanaryAutoConfiguration.java)
- **What**: `canary.kafka-heartbeat-stale-ms` and `canary.kafka-health-timeout-ms` env vars never overridden by any service.
- **Fix**: Hardcode the 15000ms defaults; remove the `@Value` injection.
- **Severity**: Low.

### P6 — Trivial wrapper exports in lib-node

- **Location**: [platform/lib-node/src/observability/lane-state-probe.ts:51-54](../platform/lib-node/src/observability/lane-state-probe.ts), [platform/lib-node/src/observability/canary-metrics-endpoint.ts](../platform/lib-node/src/observability/canary-metrics-endpoint.ts)
- **What**: `LaneStateProbe.hasAddresses()` is `static` but only called internally; `canaryMetricsEndpoint()` is a 3-line wrapper around `CanaryMetrics.getRegistry()`.
- **Fix**: Make `hasAddresses()` private. Inline `canaryMetricsEndpoint()` at the two callsites.
- **Severity**: Low.

### P7 — `restate-defs-java` abstract handler stubs are never extended

- **Location**: [platform/restate-defs-java/](../platform/restate-defs-java/)
- **What**: The library exports abstract `@Service`/`@VirtualObject`/`@Workflow` classes (`CheckoutSaga`, `PaymentVOStable`, `ReservationWorkflowStable`, etc.) — DTOs are used by services, but the abstract handler stubs have zero `extends` references anywhere.
- **Fix**: Delete the abstract stubs; keep only DTO interfaces; rename the library `restate-types-java`.
- **Severity**: Medium (reduces conceptual surface area).

---

## Services

### S1 — Java service boilerplate duplicated across audit, inventory, payment

- **Location**: 3 services × 6 files of identical-or-near-identical code:
  - `build.gradle.kts` (23 lines each, identical)
  - `*Application.java` (20 lines each, identical bootstrap)
  - `config/JacksonConfig.java` (23 lines each, identical)
  - `config/IngressClientConfig.java` (32 lines each, identical between audit + payment)
  - `kafka/KafkaProducerConfig.java` (37 lines each, identical between audit + payment)
  - `controller/InternalController.java` (24 lines each, identical between audit + payment)
  - `store/ConsumedEvent.java` (5 lines each, identical between audit + payment)
- **Why**: ~140 lines of mechanical duplication per service. Changes to e.g. Jackson serialization require 3 edits.
- **Fix**: Move config classes into `platform/lib-java` (auto-configured via Spring's `@AutoConfiguration`). Services `@Import` or just rely on classpath discovery.
- **Severity**: High (highest LOC reduction in the project; lowest risk).

### S2 — Node service `kafka.ts` duplicated across notification + order

- **Location**: [services/notification-service/src/kafka.ts](../services/notification-service/src/kafka.ts), [services/order-service/src/kafka.ts](../services/order-service/src/kafka.ts)
- **What**: ~180 lines of identical Kafka setup. Only differences: `clientId` (notification vs order) and subscribed topics.
- **Fix**: Extract `setupKafka({ clientId, topics, handler })` to `platform/lib-node`.
- **Severity**: High.

### S3 — Try-catch-rethrow wrappers in Java Restate handlers

- **Location**:
  - [services/audit-service/.../AuditQueryServiceImpl.java:32-48](../services/audit-service/src/main/java/com/canary/audit/handler/AuditQueryServiceImpl.java)
  - [services/payment-service/.../PaymentVOImplCanary.java:30-50](../services/payment-service/src/main/java/com/canary/payment/handler/PaymentVOImplCanary.java)
  - [services/payment-service/.../PaymentVOImplStable.java](../services/payment-service/src/main/java/com/canary/payment/handler/PaymentVOImplStable.java)
- **What**: Handlers wrap `meter.measure()` in `try { … } catch (RuntimeException e) { throw e; } catch (Exception e) { throw new RuntimeException(e); }`.
- **Why**: Catch blocks add no context — they only re-throw. The meter already records `success`/`server_error` outcomes; the wrapper is theater.
- **Fix**: Let exceptions propagate. Drop the try-catch.
- **Severity**: Low.

---

## CLI tools

### T1 — `canary-ctl rolloutStatus()` is dead code

- **Location**: [tools/canary-ctl/src/kubectl.ts:38-44](../tools/canary-ctl/src/kubectl.ts) + corresponding test
- **What**: Exported `rolloutStatus()` wrapping `kubectl rollout status` with timeout, never called from any command. The deploy logic uses `upgradeInstallCanary` with `--wait` instead.
- **Fix**: Delete the function and its unit test.
- **Severity**: Low.

### T2 — `canary-ctl` unused global flags

- **Location**: [tools/canary-ctl/src/index.ts:24-25](../tools/canary-ctl/src/index.ts)
- **What**:
  - `--grace-seconds <n>` — declared globally with default 10; only one consumer (rollback) and never overridden in Makefile, runbooks, or tests.
  - `--verbose` — enables one debug log in exec.ts; never exercised by tests or Makefile.
- **Fix**: Remove both flags. Hardcode 10s grace in rollback; remove the unused debug log path.
- **Severity**: Low.

### T3 — `canary-ctl` registry over-abstraction

- **Location**: [tools/canary-ctl/src/registry.ts:1-44](../tools/canary-ctl/src/registry.ts)
- **What**: Factory function `entry()` producing 5 nearly-identical service entries; wrapped in a `lookup()` accessor.
- **Why**: Not pluggable, not config-driven, not tested for dynamic registration. The factory only repeats five known service names.
- **Fix**: Replace with a plain object literal. Drop `entry()`.
- **Severity**: Low.

### T4 — `traffic-cli` thin `sendOrder()` wrapper

- **Location**: [tools/traffic-cli/src/index.ts:4-28](../tools/traffic-cli/src/index.ts)
- **What**: `sendOrder()` is a one-caller wrapper that sets headers and calls axios; `SendOrderOpts` mirrors the command options exactly.
- **Fix**: Inline into the action handler; delete the interface.
- **Severity**: Low.

---

## Deploy / infra

### D1 — Istio routing manifests: 10 nearly-identical YAMLs

- **Location**: [deploy/routing/destination-rules/](../deploy/routing/destination-rules/) (5 files × 14 lines), [deploy/routing/virtual-services/](../deploy/routing/virtual-services/) (5 files × 15 lines)
- **What**: Each DestinationRule and VirtualService varies only by service name; everything else is duplicated.
- **Fix**: Single Helm template that iterates over a list of service names. Drop the 10 standalone YAMLs.
- **Severity**: Medium.

### D2 — `canary-overlay.yaml` restates defaults it inherits

- **Location**: [deploy/helm/values/canary-overlay.yaml](../deploy/helm/values/canary-overlay.yaml)
- **What**: Sets `replicas: 1` (same as stable default) and `KAFKA_CONSUMERS_ENABLED: "true"` with a comment admitting "default from values.yaml applies."
- **Fix**: Strip to only fields that genuinely differ between stable and canary.
- **Severity**: Low.

---

## Tests

### TS1 — S2 and S4 e2e tests are functionally identical

- **Location**: [tests/e2e/s2-single-svc-canary.test.ts](../tests/e2e/s2-single-svc-canary.test.ts), [tests/e2e/s4-full-chain-canary.test.ts](../tests/e2e/s4-full-chain-canary.test.ts)
- **What**: Both deploy all 5 services as canary, send the same HTTP POST, and assert the same chain.
- **Fix**: Merge into one file with two `it()` blocks. Cuts fixture overhead 50% with no coverage loss.
- **Severity**: Medium.

### TS2 — Chain helper unit tests duplicate e2e coverage

- **Location**: [tests/e2e/helpers/__tests__/chain.test.ts](../tests/e2e/helpers/__tests__/chain.test.ts)
- **What**: Unit tests for `parseChain` / `getChain` / `assertVersion` / `assertContains` / `assertAbsent` — every function is exercised end-to-end in S2/S3/S4/S8.
- **Fix**: Delete the file (~80 lines). The e2e suite covers the same parser against real HTTP responses.
- **Severity**: Low.

---

## Docs

### DC1 — `history.md` Phase 3.b section duplicates `canary-mechanics.md`

- **Location**: [docs/history.md](history.md) (the Plan 3.b section, ~205 lines added in the most recent revision)
- **What**: Procedural explanation of Restate variant-isolated handler registration is restated almost verbatim in [canary-mechanics.md](canary-mechanics.md) §Restate-path.
- **Why**: `history.md` is for "why did we decide this?"; `canary-mechanics.md` is for "how does it work?". The Phase 3.b section blurs the boundary.
- **Fix**: Keep in `history.md`: the decision (α native vs β variant-isolated), rationale, tradeoff (no pause/resume). Cut the procedural details. Cross-link to `canary-mechanics.md`.
- **Severity**: Low (~100 lines saved).

---

## Rejected findings (do not "fix" these)

These were flagged by review agents but are intentional — documented here so future passes don't re-litigate.

1. **Kubernetes presence watcher complexity** ([XCanaryPresenceWatcher](../platform/lib-java/src/main/java/com/canary/platform/lib/XCanaryPresenceWatcher.java) and the Node equivalent): this is the *product*. The watcher coordinates stable-takeover-on-canary-failure. Removing it would gut the canary management story.
2. **In-memory stores using `CopyOnWriteArrayList`** in audit/payment: needed because the Kafka consumer thread and HTTP read thread share state. Thread-safety is correct, not over-engineered.
3. **`RestateEndpointConfig` variant-selection logic duplicated between payment and inventory**: the duplication *is* the demonstration — it shows the canary-aware handler-registration pattern is reusable. Consolidating would obscure the pattern.
4. **`canary_*` metric coverage**: covers both stable and canary lanes (see N1). Naming, not coverage, is the issue.
5. **Spring Boot Actuator + Micrometer baseline metrics**: lane-agnostic by design (`http_server_requests_seconds`, JVM, GC, Kafka client). These are the general-application observability layer that complements the lane-tagged `canary_*` metrics. Not redundant.

---

## Suggested batching

When working through this list, group like with like to minimize merge churn:

1. **High-impact deduplication** — S1 + S2 (Java + Node boilerplate to platform libs). Largest LOC reduction.
2. **Observability cleanup** — N1 + P1 + P2 + P3 (rename + delete `LaneStateProbe` + delete `TracingAutoConfiguration` + simplify `CanaryRestateMeter`). All touch the same package.
3. **CLI cleanup** — T1 + T2 + T3 + T4 in one pass.
4. **Infra cleanup** — D1 + D2 + TS1 + TS2.
5. **Docs** — DC1 standalone.
