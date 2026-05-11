# Canary Release Phase 5.a-node — Node Service Observability — Design

**Status:** approved (brainstorming → spec)
**Date:** 2026-05-11
**Predecessor:** Phase 5.a (merged 2026-05-11, `e26b201`) — Java-side instrumentation
**Parent spec:** `docs/superpowers/specs/2026-05-11-canary-release-phase-5-observability-design.md` — §1 (metrics) and §1.5 T1 (HTTP tracing) define the contract this sub-phase delivers in TypeScript.

## Goal

Bring `services/order-service` and `services/notification-service` (Node.js / TypeScript) to observability parity with the Java services that landed in 5.a. After 5.a-node, the same `canary_request_total`, `canary_request_duration_seconds`, `canary_shadow_mismatch_total`, and `canary_lane_active` metrics are emitted from these two services with identical tag sets, and Jaeger receives spans from them tagged with `canary.lane` and `canary.service`.

## Non-goals

- T2 Kafka and T3 Restate trace-context propagation across substrates — those land in 5.b on both sides simultaneously, so the Node bits stay there.
- Wiring `canaryMetrics.recordShadowMismatch(...)` at Phase 2/3 shadow-comparison call sites — also 5.b.
- A new shared "_shared" cross-service library. The work goes into the existing `platform/lib-node` package, mirroring how 5.a put everything in `platform/lib-java`.

## Mirror table — Java component → Node component

| Java (5.a) | Node (5.a-node) |
|---|---|
| `CanaryLaneTag.current()` | `currentLane(): "stable" \| "canary"` in `canary-lane-tag.ts` |
| `CanaryMetrics` (Micrometer) | `CanaryMetrics` class wrapping a `prom-client` `Registry` |
| `XCanaryRequestFilter` augmented w/ metrics | `xCanaryMiddleware` augmented + `canaryHttpMetricsMiddleware` wrapper (see §3) |
| `CanaryKafkaRecordInterceptor` (RecordInterceptor) | `wrapKafkaConsumer(consumer, metrics)` helper in `canary-kafka-metrics.ts` |
| `CanaryRestateMeter.measure(handler, body)` | `measureRestate(handler, body)` in `canary-restate-meter.ts` |
| `LaneStateProbe` (Fabric8 watcher) | `LaneStateProbe` (already-used `@kubernetes/client-node` watcher; mirrors `x-canary-presence-watcher.ts`) |
| `CanaryHttpSpanFilter` (OTel `Span.setAttribute`) | OTel `SpanProcessor` in `tracing.ts` that adds `canary.lane` to root HTTP spans |
| `CanaryMetricsAutoConfiguration` (Spring) | Per-service init in `services/{order,notification}-service/src/index.ts` (no autowire equivalent in Node — explicit init) |
| `TracingAutoConfiguration` | `tracing.ts` — initializes `NodeSDK` with OTLP gRPC exporter |
| Helm scrape annotation `prometheus.io/path: /actuator/prometheus` | Same. Node services expose prom registry at `/actuator/prometheus` (mirroring Java path so the Helm annotation needs no change) |

## Architecture

### Library scope

All shared code lives in `platform/lib-node/src/observability/`. The barrel `lib-node/index.ts` re-exports everything, matching the existing pattern (where `x-canary-middleware`, `x-canary-kafka`, etc. already export from the top level).

### Tracing initialization

OTel SDK must be initialized *before* any module to be auto-instrumented is imported. The Node convention is a tiny `tracing.ts` that's imported as the very first line of the service entry point:

```typescript
// services/order-service/src/index.ts
import "./tracing.js";   // MUST be first — initializes OTel SDK before express/kafkajs are loaded
import express from "express";
// ... rest of existing imports
```

`tracing.ts` calls `initTracing(serviceName)` from `lib-node`, which configures `NodeSDK` with:
- `@opentelemetry/auto-instrumentations-node` for HTTP + Express + KafkaJS auto-instrumentation
- `@opentelemetry/exporter-trace-otlp-grpc` pointed at `OTLP_TRACING_ENDPOINT` env var (default `http://jaeger-collector.istio-system:4317`)
- A custom `SpanProcessor` that injects `canary.lane` and `canary.service` attributes onto every span at start time. The processor reads lane from `XCanaryContext` (the existing AsyncLocalStorage frame).

### Metrics emission

A single shared `CanaryMetrics` class (analog of the Java one) wraps a `prom-client` `Registry`. The four metrics:

| Name | Type | Tags |
|---|---|---|
| `canary_request_total` | Counter | `substrate, service, lane, outcome, target` |
| `canary_request_duration_seconds` | Histogram (default buckets) | `substrate, service, lane, target` |
| `canary_shadow_mismatch_total` | Counter | `service, field` |
| `canary_lane_active` | Gauge | `substrate, service, lane` |

Tag values match Java exactly so dashboards aggregate correctly.

### HTTP metrics middleware

A new `canaryHttpMetricsMiddleware` Express middleware wraps `res.end` to capture the start time + final status code + matched route, then calls `metrics.recordHttp(target, outcome, duration)`. Registered AFTER `xCanaryMiddleware` so `XCanaryContext` is populated. The wrapping pattern is the standard Express middleware convention; it doesn't fork the existing `xCanaryMiddleware`.

### Kafka metrics

A `wrapKafkaConsumer(consumer, metrics)` helper takes a `kafkajs` `Consumer` and returns a wrapped variant whose `run({ eachMessage })` callback is timed + tagged before delegating. Existing services already wrap Kafka setup in `services/{order,notification}-service/src/kafka.ts` — we add one `wrapKafkaConsumer(...)` call at that site.

### Restate metrics

A `measureRestate<T>(handlerName, body: () => Promise<T>): Promise<T>` async function. Restate handler bodies wrap their work with it. Wiring at handler call sites is deferred to **5.b** alongside Restate trace-context propagation; this sub-phase only ships the helper.

### Lane-state gauge

`LaneStateProbe` watches the `<service>-stable` and `<service>-canary` Endpoints in the `services` namespace via `@kubernetes/client-node` (already a lib-node dep). Updates the `canary_lane_active{substrate="http", service, lane}` gauge — matches the Java probe's tag set after the substrate fix landed in 5.a (`8f43bf5`).

### Metrics endpoint

`canaryMetricsEndpoint(metrics)` returns an Express request handler that responds with the prom registry's text-format output. Each service mounts it at `GET /actuator/prometheus`. The Helm chart's existing `prometheus.io/path: /actuator/prometheus` annotation (Task 15 of 5.a) points Prometheus straight at it.

## Per-service wiring

`services/order-service/src/index.ts` and `services/notification-service/src/index.ts`:

1. Add `import "./tracing.js";` as the FIRST import.
2. Create a per-service `tracing.ts` (3 lines: `import { initTracing } from "@canary/lib-node"; initTracing("order");`).
3. Add `import { CanaryMetrics, canaryHttpMetricsMiddleware, canaryMetricsEndpoint, LaneStateProbe } from "@canary/lib-node";`.
4. After `setupHttp(...)`, register: `app.use(canaryHttpMetricsMiddleware(metrics))` and `app.get("/actuator/prometheus", canaryMetricsEndpoint(metrics))`.
5. After `setupKafka(...)`, replace direct consumer use with `wrapKafkaConsumer(consumer, metrics)`.
6. Start `LaneStateProbe` and tear it down on shutdown (the existing `shutdown` handler in each `index.ts` is the right hook).

## Sub-phase scope discipline

**In scope (must ship in this PR):**
- `lib-node/src/observability/` package + tests
- `platform/lib-node/index.ts` barrel additions
- Per-service `tracing.ts` + `index.ts` wiring for both Node services
- The two Node services build clean (`pnpm build`) and tests pass (`pnpm test`)
- `package.json` updates for both lib-node and the two services

**Out of scope (handed to 5.b):**
- Restate handler call-site wiring of `measureRestate`
- Shadow-mismatch counter call-site wiring
- T2 Kafka producer/consumer trace-context (KafkaJS auto-instrumentation handles span linkage; explicit `traceparent` injection across consumer-group boundaries is 5.b)
- Restate inter-handler trace-context propagation

**Out of scope (handed to 5.c):**
- Recording rules / alerting rules
- Alertmanager + alert-sink

## Risks

| Risk | Mitigation |
|---|---|
| OTel SDK initializes after some module is already loaded → that module is not auto-instrumented | Strict convention: `import "./tracing.js"` is the FIRST line of every entry point. Document in `tracing.ts` jsdoc. |
| `@opentelemetry/auto-instrumentations-node` is heavy (pulls many transitive deps) | Acceptable — these are dev-only services; the bundle weight for a deployed Docker image is negligible vs the JVM image. |
| KafkaJS auto-instrumentation does NOT emit Kafka spans on the receiver-side `eachMessage` callback (known OTel-Node gap in some versions) | Verified at task time; if missing, fall back to manual span creation in `wrapKafkaConsumer`. Documented as a verification step in the plan. |
| `prom-client` default registry is process-global; multiple `CanaryMetrics` instances in the same process collide on metric names | `CanaryMetrics` accepts an optional `Registry` constructor arg; default is a new `Registry`. Per-service we create one; tests construct fresh registries. |
| Restate handler invocations come in over HTTP — auto-HTTP instrumentation will create spans, but the `canary.lane` SpanProcessor only fires on root spans, missing handler-internal child spans | Acceptable for 5.a-node — the root span carries the lane. Handler-body wrapping (`measureRestate`) is 5.b. |

## Operational notes

- Tracing endpoint defaults to `http://jaeger-collector.istio-system:4317`. Override via `OTLP_TRACING_ENDPOINT` env var.
- Metrics endpoint exposed at `/actuator/prometheus` on the same HTTP port as the service's main API. No new port. No auth.
- `LaneStateProbe` requires the pod's ServiceAccount to have `endpoints` `get`/`list`/`watch` permission in the `services` namespace. The existing `XCanaryPresenceWatcher` already has this RBAC granted, so no manifest changes needed.

## Out-of-scope (explicit, for plan-writing clarity)

- No changes to the Helm chart, RBAC, or any deploy manifests. The 5.a Helm scrape annotation is generic and points at `/actuator/prometheus`, which Node will now expose.
- No new container ports.
- No changes to Java services or `lib-java`.
- No Phase 4 / Phase 5.b / 5.c / 5.d work.
