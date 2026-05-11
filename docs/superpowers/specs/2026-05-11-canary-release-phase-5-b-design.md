# Canary Release Phase 5.b — Trace Propagation + Restate Handler Metric Wiring — Design

**Status:** approved (brainstorming → spec)
**Date:** 2026-05-11
**Predecessors:**
- Phase 5.a (Java foundation, merged 2026-05-11, `e26b201`)
- Phase 5.a-node (Node foundation, merged 2026-05-11, `3f30eae`)
**Parent spec:** `docs/superpowers/specs/2026-05-11-canary-release-phase-5-observability-design.md` — §1.5 T2 (Kafka tracing) and §1.5 T3 (Restate tracing) are the design surface this sub-phase delivers.

## Goal

Make a single end-to-end canary request **observable as one connected trace in Jaeger** as it crosses HTTP → Kafka → Restate → Kafka boundaries between Java and Node services, AND make every Restate handler invocation visible as a per-handler latency/outcome metric (`canary_request_total{substrate="restate"}`, `canary_request_duration_seconds{substrate="restate"}`).

After 5.b, an operator filtering Jaeger by `canary.lane=canary` can see a complete cross-service trace and click into spans to find handler-level metrics in Grafana that align with the trace timeline.

## Non-goals

- **Recording rules, SLOs, alerting, dashboards** — Phase 5.c and 5.d.
- **Shadow-read comparison metric (`canary_shadow_mismatch_total`)** — the spec assumed Phase 2/3 implemented shadow reads at specific call sites. They did not — Phase 2/3 implemented *header-routed lane isolation* where stable and canary process disjoint inputs. There are no shadow-comparison sites to wire. The helper (`CanaryMetrics.recordShadowMismatch` in Java, `metrics.recordShadowMismatch` in Node) stays unused until a future sub-phase implements true shadow reads.
- **OTel collector / Tempo / Loki** — direct OTLP to Jaeger as in 5.a.
- **Cross-cluster Restate.** Single-cluster.

## Mirror table — what changes per substrate × language

| Substrate | Java change | Node change |
|---|---|---|
| **HTTP** (already done in 5.a + 5.a-node) | Auto-instrumented by OTel Spring starter; `canary.lane` filter set on entry. | `@opentelemetry/instrumentation-http` auto-instruments; `CanaryLaneSpanProcessor` adds attributes. |
| **Kafka** | Set `observationEnabled = true` on `ConcurrentKafkaListenerContainerFactory.containerProperties` and on every `KafkaTemplate` bean. Spring Kafka 4.0.4 then emits W3C `traceparent` into producer record headers and starts a child span on consumer entry. | `@opentelemetry/instrumentation-kafkajs` is included by `@opentelemetry/auto-instrumentations-node` (verified via task 0). It auto-injects `traceparent` on `producer.send` and starts spans on `consumer.run({ eachMessage })`. **If verification shows it does not work in our pinned version, fall back to manual span wrap inside `wrapKafkaConsumer` (already exists from 5.a-node) + a producer wrap helper.** |
| **Restate** | `RESTATE_TRACING_ENDPOINT` env on the Restate StatefulSet → runtime emits OTLP spans. Java SDK 2.7.0 reads incoming `traceparent` from invocation HTTP headers and propagates onto `ctx.call(...)` / `ctx.send(...)` invocation metadata. **Verification of this propagation is task 0 of 5.b** — if the SDK does not propagate, manually inject `traceparent` via the existing `XCanaryRestateClientCustomizer` pattern. | Same `RESTATE_TRACING_ENDPOINT` change applies. Node SDK 1.14.2 — the Restate Node SDK uses HTTP for ingress + handler calls, so OTel HTTP instrumentation should pick it up. Verify with the same task-0 check; fall back to manual `traceparent` injection in `applyXCanaryToRestateOptions` (existing helper in lib-node) if needed. |

## Restate handler measure() wiring

Every Restate handler in the codebase gets wrapped with the per-handler meter helper from 5.a/5.a-node:
- Java: `restateMeter.measure(handlerName, () -> body)` (where `restateMeter` is the `CanaryRestateMeter` bean injected from `CanaryMetricsAutoConfiguration`)
- Node: `await measureRestate(metrics, handlerName, async () => body)` (where `metrics` is the per-service `CanaryMetrics` instance)

### Discovered handler inventory

| Service | Language | Handler class / fn | Handler entry methods |
|---|---|---|---|
| payment-service | Java | `PaymentVOImplStable`, `PaymentVOImplCanary` | `charge(req)`, `refund(req)` |
| inventory-service | Java | `ReservationWorkflowImplStable`, `ReservationWorkflowImplCanary` | `run(req)` (and any other handler entries on the workflow) |
| audit-service | Java | `AuditQueryServiceImpl` | `append(event)`, `query(req)` (and any other entries on the service) |
| order-service | Node | `services/order-service/src/restate.ts` | `checkoutSagaRunHandler` (workflow `run`) |
| notification-service | Node | `services/notification-service/src/restate.ts` | `notifyHandler`, `auditQueryHandler` (whatever else is registered) |

The handler-name string used in `measure(...)` follows the convention `<ServiceName>.<methodName>` matching the `target` tag the spec defines (e.g. `"PaymentVOStable.charge"`). This matches the Java SDK 2.7.0 / Node SDK 1.14.2 published handler names.

## T2 Kafka trace-context — Java details

`platform/lib-java/src/main/java/com/canary/platform/lib/autoconfigure/XCanaryAutoConfiguration.java` already owns the Kafka listener container factory bean (modified in 5.a Task 11). Add:

```java
factory.getContainerProperties().setObservationEnabled(true);
```

Each service that defines its own `KafkaTemplate` bean (`payment-service`, `audit-service`, `inventory-service`, all in `kafka/KafkaProducerConfig.java`) gets:

```java
template.setObservationEnabled(true);
```

Spring Kafka 4.0.4 + Micrometer Tracing 1.6.x (already on the classpath from 5.a Task 1) handles the rest: producer side stamps `traceparent` into record headers, consumer side extracts it and creates a child span linked to the producer span.

## T2 Kafka trace-context — Node details

Verification flow as task 0 of the Kafka section:

1. Run a smoke test that produces and then consumes a message.
2. Check Jaeger for two spans named `kafka send` and `kafka receive` (or similar — names are produced by the OTel kafkajs instrumentation).
3. Verify the `receive` span's parent points back to the `send` span.

If verification passes — no code change. If it fails:
- Add `injectTraceContextIntoKafkaProducer` helper to `lib-node/src/observability/canary-kafka-metrics.ts` that wraps `producer.send` and stamps `traceparent` from the active span context.
- Augment `wrapKafkaConsumer` to extract `traceparent` from the message headers, restore the OTel context for the duration of `eachMessage`, and start a span linked to the parent.

## T3 Restate trace-context

### Restate StatefulSet env

Edit `deploy/kind/restate/statefulset.yaml`:

```yaml
env:
  - name: RUST_LOG
    value: info
  - name: RESTATE_TRACING_ENDPOINT
    value: "http://jaeger-collector.istio-system:4317"
  - name: RESTATE_TRACING_FILTER
    value: "info,restate=info"
```

Removes the comment-flagged 1.6.x empty-string panic. Restate runtime emits OTLP spans for every handler invocation, journal entry, and inter-handler call.

### Java SDK propagation verification (task 0 of Restate section)

Test scenario:
1. From a Java service's HTTP entry point, get the active OTel span.
2. Make a Restate `ctx.call(otherHandler, args)` — this call hops through the Restate runtime to another handler.
3. In the destination handler, read the active OTel span's parent.
4. Assert the parent is the call site's span.

If the assertion holds → SDK propagates W3C trace-context natively. No code change.
If it fails → The Java SDK 2.7.0 invocation metadata API (`InvocationOptions.headers`) is already used by `XCanaryRestateClientCustomizer` to stamp `x-canary`. Extend that customizer to also stamp `traceparent` from the active OTel span context.

### Node SDK propagation verification

Same flow; same fallback location: `applyXCanaryToRestateOptions` in `lib-node/src/x-canary-restate.ts` already injects per-call headers — extend it to also inject `traceparent` if needed.

## Scope discipline

**In scope (must ship in this PR):**
- T2 Kafka observation enabled (Java) + verification on Node (with manual fallback if needed)
- T3 Restate runtime tracing env + propagation verification (with manual fallback if needed)
- `restateMeter.measure(...)` / `measureRestate(...)` wrapping every Restate handler in all 5 services
- Lib changes: extend the existing `XCanaryRestateClientCustomizer` (Java) and `applyXCanaryToRestateOptions` (Node) ONLY if verification shows native propagation does not work
- Helm/RBAC: no changes (assumes Restate StatefulSet manifest patch lands cleanly)

**Out of scope (handed to 5.c):**
- Recording rules, alerts, alertmanager, alert-sink

**Out of scope (handed to 5.d):**
- Dashboards, runbooks, E2E tests

**Out of scope (no future plan yet):**
- `recordShadowMismatch` call-site wiring — there are no shadow-comparison sites in the codebase. Helper stays unused until a future sub-phase implements true shadow reads.

## Risks

| Risk | Mitigation |
|---|---|
| Restate SDK 2.7.0 (Java) does not propagate W3C trace-context across `ctx.call` | Task 0 verifies; fallback is to extend `XCanaryRestateClientCustomizer` to inject `traceparent` (small, scoped). |
| KafkaJS auto-instrumentation does not emit consumer-side spans on `eachMessage` | Task 0 verifies; fallback is manual span wrap inside `wrapKafkaConsumer`. |
| `setObservationEnabled(true)` on Spring Kafka has runtime overhead | Acceptable — dev cluster. Can be made conditional on a property if needed. |
| `RESTATE_TRACING_ENDPOINT` change requires Restate StatefulSet rollout; in-flight invocations may be disrupted | Document in operational notes; rollout in dev clusters has no SLA. |
| Restate handler method names may not exactly match the spec's `target` tag convention | Use SDK-published handler names verbatim (`def.name` for Node, `Restate*.handler-method` for Java). Document the actual target string per handler. |

## Operational notes

- T2 + T3 are observability-only changes — no behavior change, no schema change, no API break.
- The Java `XCanaryAutoConfiguration` bean factory already supplies `CanaryRestateMeter` (per 5.a Task 10). Each Restate handler implementation needs the meter injected via constructor.
- Node Restate handlers in `services/{order,notification}-service/src/restate.ts` need access to the per-service `CanaryMetrics` instance. The instance is constructed in `index.ts` (5.a-node) — pass it through `setupRestate(...)` options.
- After 5.b, `canary_request_total{substrate="restate"}` will appear in Prometheus with `target=<handler-name>` rows for every invoked handler. Per-handler dashboard slicing becomes possible.

## Out-of-scope (explicit)

- No changes to the Helm chart (5.a Task 15 already added scrape annotations).
- No new env vars on application services (only on the Restate StatefulSet).
- No changes to Phase 1/2/3 routing semantics.
- No changes to `lib-java`/`lib-node` public observability surface beyond the already-shipped 5.a/5.a-node API. Extensions only happen IF the verification fallbacks are needed.
