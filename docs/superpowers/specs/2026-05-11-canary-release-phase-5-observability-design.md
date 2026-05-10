# Canary Release Phase 5 — Observability Polish — Design

**Status:** approved (brainstorming → spec)
**Date:** 2026-05-11
**Predecessors:** Phase 1 (HTTP), Phase 2 (Kafka), Phase 3 (Restate). All merged.
**Skipped predecessor:** Phase 4 (CI/CD + percent-split + Argo Rollouts) is intentionally deferred. Phase 5 assumes 0%/100% lane routing as built in Phases 1–3.

## Goal

Make every canary release in this stack observable end-to-end. After Phase 5, an operator with `kubectl` access to a kind cluster can:

- Open Grafana and see, on one dashboard, the side-by-side health of every service's stable and canary lane across HTTP, Kafka, and Restate substrates.
- Receive an alert (visible in `kubectl logs deploy/alert-sink`) within minutes of a canary lane breaching its SLO budget.
- Open Jaeger, filter by `canary.lane=canary`, and follow a single request's trace as it crosses HTTP → Kafka → Restate hops, with each span attributed to the lane that handled it.
- Open one of six runbooks and walk through a structured response to the alert that just fired.

## Non-goals

- **Promotion gating.** SLOs are observational only. `canary-ctl promote` does not query Prometheus and does not refuse to promote on budget burn. (That belongs to the deferred Phase 4.)
- **Percent-split SLO accounting.** Lanes are 0%/100%; no traffic-shaping math. SLO denominators are raw request counts per lane.
- **Production-grade observability infra.** Single-replica Prometheus / Grafana / Jaeger / Alertmanager, no HA, no long-term storage, no external paging.
- **Whole-JVM lane tagging.** JVM/GC/HTTP-client metrics are NOT lane-tagged. Only the four canary-specific metrics defined in §1 carry `lane`.
- **External alert receivers** (Slack, PagerDuty, email). Alerts terminate at an in-cluster `alert-sink` log receiver.
- **Custom OTel collector / Tempo / Loki.** Spans go directly to Jaeger via OTLP; logs stay on `kubectl logs`.
- **Synthetic load generation as part of Phase 5 deliverable.** Existing E2E scenarios from Phase 1.5 supply traffic.

## Alternatives considered

### α — Lane-tag every Micrometer meter via a global `MeterFilter` (rejected)

A `MeterFilter.commonTags("lane", currentLaneFromContext())` registered globally tags every counter/timer/gauge — JVM, GC, HTTP-client, JDBC, etc. Operators get free per-lane visibility on every metric Spring Boot already emits.

**Why rejected:**
- The lane is request-scoped (lives in `XCanaryContext`), but many meters are sampled outside any request (JVM GC, scheduler thread). The "current lane" is undefined for them, forcing a fallback tag like `lane=none` that pollutes the cardinality without being useful.
- Lane discrimination on JVM metrics is rarely actionable — if the canary pod has a memory leak, you see it on per-pod metrics already (`pod=...-canary-...`); a `lane` tag adds nothing.
- Doubles metric cardinality across the entire metric surface for limited operational value.

### β — Surgical canary-specific metrics at the four boundaries (chosen)

Define four explicit canary-aware metrics (§1) and emit them at the three substrate entry points + the shadow-comparison sites that already exist. JVM/GC/HTTP-client metrics stay vanilla. Lane discrimination on incidental metrics happens via the `pod` label (since stable/canary deployments have distinct pod-name prefixes).

**Trade-offs accepted:**
- No lane-tagged JVM metrics. Acceptable: incidental anomalies show up via per-pod labels.
- Four metrics cover only the canary observation surface; non-canary HTTP metrics from Istio sidecars remain the source of truth for general request metrics.

**Why chosen:** the metrics that matter for canary decisions are the canary-specific ones. β ships them with minimal cardinality impact and minimal code surface.

### γ — Cap tracing at T1+T2 (HTTP+Kafka), accept Restate inter-handler trace gap (rejected)

Skip Restate trace propagation. Operators see HTTP and Kafka spans tagged with lane in Jaeger, but Restate handler hops appear as disconnected sub-trees.

**Why rejected:** Restate already supports OTLP export natively (the existing `statefulset.yaml` comment confirms `RESTATE_TRACING_ENDPOINT` is recognized; only the empty-string regression blocked it). Setting the env to Jaeger's OTLP endpoint costs one line. Restate's Java SDK 2.7.0 carries W3C trace-context across `ctx.call(...)` invocations natively. There is no engineering reason to skip T3.

## Architecture

### Component map after Phase 5

```
┌─────────────────────────────────── kind cluster ─────────────────────────────────┐
│                                                                                  │
│  services namespace                              istio-system namespace          │
│  ┌──────────────────────────┐                    ┌────────────────────────────┐  │
│  │ {payment,audit,inventory │  Prometheus scrape │ prometheus (Istio addon)   │  │
│  │  ,order,notification}    │ ─────────────────► │  + recording rules         │  │
│  │  -{stable,canary}        │  /actuator/        │  + alerting rules          │  │
│  │                          │   prometheus       │                            │  │
│  │  Spring Boot 4 + Actuator│                    └─────────┬──────────────────┘  │
│  │  Micrometer w/           │                              │ alerts             │
│  │   - canary metrics (β)   │  OTLP traces (gRPC :4317)    ▼                    │
│  │   - tracing bridge OTel  │ ───────────────────►┌────────────────────────────┐ │
│  └──────────────────────────┘                     │ alertmanager               │ │
│                                                   │ (new, single replica)      │ │
│  restate namespace                                └─────────┬──────────────────┘ │
│  ┌──────────────────────────┐                               │ webhook            │
│  │ restate (1.6.2)          │  OTLP traces                  ▼                    │
│  │  RESTATE_TRACING_ENDPOINT│ ─────────────────► ┌────────────────────────────┐  │
│  │  → jaeger:4317           │                    │ alert-sink (new)           │  │
│  └──────────────────────────┘                    │  POST /alerts → log stdout │  │
│                                                  └────────────────────────────┘  │
│  istio-system (existing addons, extended)                                        │
│  ┌──────────────────────────┐  ┌──────────────────────────┐                      │
│  │ grafana                  │  │ jaeger                   │                      │
│  │  + 5 dashboard JSONs     │  │  + OTLP receiver enabled │                      │
│  │    via sidecar loader    │  │  + canary.lane attr      │                      │
│  └──────────────────────────┘  └──────────────────────────┘                      │
└──────────────────────────────────────────────────────────────────────────────────┘
```

### Sub-phase decomposition

Phase 5 is too large for a single PR. The implementation plan will split into four sub-phases, each independently mergeable:

| Sub-phase | Scope | Touches |
|---|---|---|
| **5.a** | Instrumentation foundation (β metrics + T1 tracer bridge + service config) | `platform/lib-java`, all 5 service `application.yml`, no infra |
| **5.b** | Tracing propagation across substrates (T2 Kafka + T3 Restate) | `platform/lib-java`, `deploy/kind/restate/statefulset.yaml`, Kafka consumer factory wiring |
| **5.c** | Recording rules + SLOs + Alertmanager + alert-sink | `deploy/kind/observability/`, new `services/alert-sink/` |
| **5.d** | Dashboards + runbooks + E2E test | `deploy/kind/observability/dashboards/`, `docs/runbooks/`, `tests/e2e/` |

## §1 — Metric instrumentation (data layer)

A new package `com.canary.platform.lib.observability` in `platform/lib-java` provides one Spring auto-configuration class (`CanaryMetricsAutoConfiguration`) that registers a fixed set of Micrometer instruments. Every instrument carries three required tags: `lane` (`stable`/`canary`), `service` (e.g. `payment`), `substrate` (`http`/`kafka`/`restate`). One optional tag — `target` — disambiguates per-substrate work units (handler name for Restate, topic for Kafka, controller-mapping for HTTP).

### Metric definitions

| Metric name | Type | Tags | Emission site |
|---|---|---|---|
| `canary_request_total` | counter | required: `substrate, service, lane, outcome`; optional: `target` | HTTP filter (existing `XCanaryHeaderFilter` augmented; `target`=request mapping pattern), Kafka consumer post-process (`XCanaryAutoConfiguration` listener wrapper; `target`=topic), Restate handler invoke (new `CanaryRestateInterceptor` base class; `target`=handler name) |
| `canary_request_duration_seconds` | timer (histogram, p50/p95/p99 + native histogram) | required: `substrate, service, lane`; optional: `target` | same three sites; `Timer.Sample` start at entry, stop at exit |
| `canary_shadow_mismatch_total` | counter | `service, field` | wherever Phase 2/3 shadow-read comparison runs (Kafka shadow-consume + Restate shadow-handler-call) |
| `canary_lane_active` | gauge (0/1) | `substrate, service, lane` | reported by `canary-ctl`-applied `Service` selector state — discovered via a new `LaneStateProbe` that polls k8s API for `Service` selectors and reports 1 when both stable and canary subsets are routable |

`outcome` ∈ `{success, client_error, server_error}` so error-rate SLOs derive without a second metric. Exception → `server_error`; HTTP 4xx → `client_error`; HTTP 2xx/3xx → `success`. For Kafka and Restate, `success` = handler returned without exception; `server_error` = exception bubbled to the framework; `client_error` is unused.

The `target` tag is bounded: HTTP request-mapping patterns are a fixed set per service (typically <50), Kafka topic names are configured (a handful), Restate handler names come from the saga (4 today). Cardinality stays well under control. Recording rules in §2 sum across `target` for SLI computation, so adding `target` does not alter SLO semantics — it only enables drill-down dashboards.

### Wire-up

- HTTP: existing `XCanaryHeaderFilter` in `lib-java` gets a `Timer.Sample.start(registry)` at the top, `.stop(registry.timer(...))` in a `finally`, and a `counter(...).increment()` after status is known.
- Kafka: `XCanaryAutoConfiguration` already wraps listener containers; add a `RecordInterceptor` that times processing and increments the counter post-`onMessage`.
- Restate: introduce `CanaryRestateInterceptor` that handlers wrap their bodies with (`return interceptor.measure(ctx, "handlerName", lane, () -> ...)`). Per-handler opt-in; the four saga handlers (CheckoutSaga, ReservationWorkflow, PaymentVO, NotificationService) all opt in.
- Lane resolution: read from `XCanaryContext.currentLane()`. Already plumbed by Phases 1–3.

### Scrape plumbing

Each service's deployment template adds:
```yaml
metadata:
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "8081"           # actuator port
    prometheus.io/path: "/actuator/prometheus"
```
The Istio Prometheus already ships with the prometheus.io/scrape relabel config, so no Prometheus reconfig is needed for service discovery.

## §1.5 — Distributed tracing

Goal: in Jaeger, search `canary.lane=canary` and see the full multi-hop trace for any request that touched the canary lane on any service, across all three substrates.

### T1 — App-layer tracer + HTTP propagation

Add to `gradle/libs.versions.toml` and `platform/lib-java/build.gradle.kts`:
- `io.micrometer:micrometer-tracing-bridge-otel`
- `io.opentelemetry:opentelemetry-exporter-otlp` (gRPC variant)
- `io.opentelemetry.instrumentation:opentelemetry-spring-boot-starter` (auto-instrumentation for HTTP server/client)

Spring Boot 4 autoconfigures the tracer, exporter, and W3C `traceparent` propagation. Application config in each service:
```yaml
management:
  tracing:
    sampling.probability: 1.0    # dev cluster — sample everything
  otlp:
    tracing:
      endpoint: http://jaeger-collector.istio-system:4317
      transport: grpc
```

The HTTP filter (already present in `lib-java`) adds:
```java
Span.current().setAttribute("canary.lane", lane.name().toLowerCase());
Span.current().setAttribute("canary.service", serviceName);
```

### T2 — Kafka trace-context propagation

Use Spring's built-in Micrometer Kafka observability:
```java
ConcurrentKafkaListenerContainerFactory<?, ?> factory = ...;
factory.getContainerProperties().setObservationEnabled(true);
KafkaTemplate<?,?> template = ...;
template.setObservationEnabled(true);
```
Producer injects `traceparent` into Kafka record headers; consumer extracts and creates a child span. The existing Kafka listener wrapper in `XCanaryAutoConfiguration` adds `canary.lane` to the active span at message receipt.

### T3 — Restate trace-context propagation

Two layers:

1. **Restate runtime spans.** Set on the Restate StatefulSet:
   ```yaml
   env:
     - name: RESTATE_TRACING_ENDPOINT
       value: "http://jaeger-collector.istio-system:4317"
     - name: RESTATE_TRACING_FILTER
       value: "info,restate=info"
   ```
   Removes the comment-flagged 1.6.x empty-string panic. Restate emits OTLP spans for every handler invocation, journal entry, and inter-handler call.

2. **App-layer Restate handler spans.** Restate's Java SDK 2.7.0 propagates W3C `traceparent` across `ctx.call(...)` and `ctx.send(...)` invocations natively (the metadata is carried in invocation headers). The `CanaryRestateInterceptor` from §1 also adds `canary.lane` and `canary.handler` attributes to the active span at handler entry.

**Risk + mitigation:** if SDK 2.7.0 turns out NOT to propagate trace-context across `ctx.call` cleanly (verification step in plan), we degrade T3 to "runtime spans only" — operators still see the Restate-side picture, just without app-level continuity across handler hops. Verification is the first task of sub-phase 5.b.

### Backend

Jaeger 1.35+ accepts OTLP on `:4317` (gRPC) and `:4318` (HTTP). The Istio addon installs a Jaeger version that supports this; `jaeger-config-patch.yaml` enables the OTLP receiver if disabled by default. No Tempo, no separate OTel collector.

## §2 — Recording rules and SLO definitions

A new `PrometheusRule`-style YAML at `deploy/kind/observability/rules/canary-recording-rules.yaml`. Three SLI families computed per `(service, substrate, lane)` over rolling windows (5m, 30m, 1h, 6h, 1d).

### SLI definitions

| SLI | Recording rule (5m window shown; same for 30m/1h/6h/1d) | SLO target |
|---|---|---|
| **Availability** | `canary:availability:ratio_5m = 1 - (sum(rate(canary_request_total{outcome="server_error"}[5m])) by (service,substrate,lane) / sum(rate(canary_request_total[5m])) by (service,substrate,lane))` | ≥ 99.0% over 30d |
| **Latency p99** | `canary:latency_p99_seconds:5m = histogram_quantile(0.99, sum(rate(canary_request_duration_seconds_bucket[5m])) by (le, service, substrate, lane))` | ≤ 0.5s for `substrate∈{http,restate}`, ≤ 2.0s for `substrate=kafka` |
| **Shadow correctness** | `canary:shadow_mismatch:ratio_1h = sum(rate(canary_shadow_mismatch_total[1h])) by (service, field) / sum(rate(canary_request_total{lane="canary"}[1h])) by (service)` | ≤ 0.1% over 1h |

### Lane-drift SLI

Operationally distinct from SLO compliance — designed to catch canary regressions before the SLO budget burns:

```promql
canary:lane_drift:p99_ratio_30m =
  (canary:latency_p99_seconds:30m{lane="canary"} / on(service,substrate) canary:latency_p99_seconds:30m{lane="stable"}) - 1
canary:lane_drift:error_ratio_30m =
  (canary:availability:ratio_30m{lane="stable"} - on(service,substrate) canary:availability:ratio_30m{lane="canary"})
```
Drift > 0.25 (p99) or > 0.005 (error rate, i.e. canary 0.5pp worse) for 30m fires `CanaryLaneDrift` (§3).

### Burn-rate alerts

Multi-window multi-burn-rate per Google SRE Workbook, computed per `(service, substrate, lane)`:

| Window pair | Burn rate threshold | Severity | Notes |
|---|---|---|---|
| 5m AND 1h | ≥ 14.4 | page (fast) | 2% of 30d budget burned in 1h |
| 30m AND 6h | ≥ 6 | ticket (slow) | 5% of 30d budget burned in 6h |

## §3 — Alert rules and routing

File: `deploy/kind/observability/rules/canary-alerts.yaml`.

| Alert name | Severity | Trigger | Notes |
|---|---|---|---|
| `CanaryAvailabilityFastBurn` | page | 5m+1h availability burn ≥ 14.4 on `lane="canary"` | one per (service, substrate) |
| `CanaryAvailabilitySlowBurn` | ticket | 30m+6h burn ≥ 6 on `lane="canary"` | |
| `CanaryLatencyFastBurn` | page | latency burn ≥ 14.4 over 5m+1h | |
| `CanaryLatencySlowBurn` | ticket | latency burn ≥ 6 over 30m+6h | |
| `CanaryLaneDrift` | warn | `canary:lane_drift:p99_ratio_30m > 0.25` OR `canary:lane_drift:error_ratio_30m > 0.005` for 30m | |
| `CanaryShadowMismatchHigh` | warn | `canary:shadow_mismatch:ratio_1h > 0.001` for 1h | |
| `CanaryLaneStuck` | warn | `max_over_time(canary_lane_active{lane="canary"}[24h]) == 1 AND canary_lane_active offset 24h == 1` (same revision, 24h+) | catches forgotten canaries |
| `StableAvailabilityFastBurn` | page | same shape but `lane="stable"` | catches baseline regressions |
| `StableAvailabilitySlowBurn` | ticket | | |
| `StableLatencyFastBurn` | page | | |
| `StableLatencySlowBurn` | ticket | | |
| `RestateInvocationFailureSpike` | warn | from Restate runtime metrics: `restate_invocations_failed_total` ratio > 0.05 over 5m | substrate-specific, not lane-tagged (Restate runtime is shared) |

### Alertmanager + alert-sink

**Alertmanager** — single-replica `Deployment` in `istio-system` (`deploy/kind/observability/alertmanager/`). Config:
```yaml
route:
  receiver: alert-sink
  group_by: [alertname, service, lane]
  group_wait: 10s
  group_interval: 1m
  repeat_interval: 1h
receivers:
  - name: alert-sink
    webhook_configs:
      - url: http://alert-sink.istio-system:8080/alerts
        send_resolved: true
```

**Prometheus → Alertmanager wiring** — patch the Istio Prometheus ConfigMap to add `rule_files: [/etc/rules/*.yaml]` (mounted from a new ConfigMap holding §2+§3 rules) and the `alerting.alertmanagers` block pointing at `alertmanager.istio-system:9093`. Restart Prometheus pod to reload. Documented as `prometheus-config-patch.yaml`.

**alert-sink** — new tiny service at `services/alert-sink/`. Single Spring Boot 4 application with one controller:
```java
@PostMapping("/alerts")
public void receive(@RequestBody String body) {
    log.info("ALERT_RECEIVED {}", body);   // structured log: each call is one line
}
```
Exposes `/actuator/health` for the readiness probe. ~50 LOC, no external deps beyond `spring-boot-starter-web` + `spring-boot-starter-actuator`. Operators run `kubectl logs -n istio-system deploy/alert-sink -f` to watch the pipeline.

## §4 — Dashboards

Grafana JSON files in `deploy/kind/observability/dashboards/`, loaded into Grafana via a ConfigMap mounted by the Istio Grafana addon's dashboard provider. Five dashboards:

### `canary-overview.json`

Single-pane "is anything wrong" view.
- **Top row**: per-service status tiles (5 tiles). Each tile derives green/yellow/red from an expression like `min by (service) (canary:availability:burn_rate_1h)` thresholded against the SLO budget.
- **Middle**: stable-vs-canary side-by-side. Two columns, three rows (one per substrate). Each cell: dual-line latency p99 + dual-bar error rate.
- **Bottom**: active canary lanes table (`canary_lane_active{lane="canary"} == 1`) with revision label and time-since-routable.

### `canary-http.json`

HTTP substrate detail. Per-service rows, each row has:
- Request rate (stable vs canary, dual-line)
- Error rate (stable vs canary, dual-line)
- Latency heatmap (canary), latency heatmap (stable), side-by-side
- HTTP status code breakdown stacked area, per lane

### `canary-kafka.json`

Kafka substrate detail. Per-service rows:
- Processed rate, stable vs canary
- Processing latency p50/p99, stable vs canary
- Consumer lag (from Kafka client metrics already exposed by `XCanaryAutoConfiguration` lines 110-114, with `lane` tag added via `MeterFilter`)
- Per-topic processing error rate, stable vs canary

### `canary-restate.json`

Restate substrate detail. Per-handler rows (CheckoutSaga, ReservationWorkflow, PaymentVO, NotificationService — selected via the `target` tag from §1):
- Invocations/sec, stable vs canary (from `canary_request_total{substrate="restate", target="<handler>"}`)
- p99 invoke duration, stable vs canary (from `canary_request_duration_seconds{substrate="restate", target="<handler>"}`)
- Failure rate at handler-level (from `canary_request_total{outcome="server_error"}`) AND at Restate-runtime-level (from Restate's exported `restate_invocations_failed_total` runtime metric, scraped from the Restate admin port via a Prometheus ServiceMonitor — see Restate runtime metrics endpoint at `:9070/metrics`)
- Active invocations gauge (from Restate's `restate_invocations_active` runtime metric, same scrape source)

### `canary-traces.json`

Operator-facing trace search. Embedded Jaeger search panels:
- "Recent canary traces" — `service.name = order-service AND canary.lane = canary`, last 1h
- "Recent stable traces (for comparison)" — same but `lane=stable`
- "Failed canary traces" — same filter + `error=true`
- One link panel per service with pre-built Jaeger query URLs

## §5 — Runbooks

Six markdown files in `docs/runbooks/`. Common template:
```markdown
## Symptom
## Triage queries (PromQL/Jaeger links + dashboard panels)
## Likely causes
## Mitigation steps
## Validation (how to confirm mitigation worked)
## Postmortem prompts
```

| Runbook | Triggered by |
|---|---|
| `canary-burning-budget.md` | `Canary{Availability,Latency}{Fast,Slow}Burn` |
| `canary-lane-drift.md` | `CanaryLaneDrift` |
| `canary-shadow-mismatch.md` | `CanaryShadowMismatchHigh` |
| `canary-lane-stuck.md` | `CanaryLaneStuck` |
| `baseline-regression.md` | `Stable{Availability,Latency}{Fast,Slow}Burn` |
| `restate-invocation-failure-spike.md` | `RestateInvocationFailureSpike` |

Each runbook references concrete `canary-ctl` commands (e.g. `canary-ctl rollback payment`), specific dashboard panels by name, and Jaeger query URLs. Every fired alert's webhook payload includes a `runbook_url` annotation pointing to the matching `docs/runbooks/*.md` file.

## §6 — Repository layout and install pipeline

```
deploy/kind/observability/
  install.sh                         # extended
  alertmanager/
    deployment.yaml
    configmap.yaml
    service.yaml
  alert-sink/
    deployment.yaml
    service.yaml
  rules/
    canary-recording-rules.yaml
    canary-alerts.yaml
    rules-cm.yaml                    # ConfigMap wrapping the two above
  dashboards/
    canary-overview.json
    canary-http.json
    canary-kafka.json
    canary-restate.json
    canary-traces.json
    dashboards-cm.yaml               # ConfigMap with sidecar label for Grafana auto-load
  prometheus-config-patch.yaml       # adds rule_files + alertmanager forward
  jaeger-config-patch.yaml           # ensures OTLP :4317 receiver enabled

platform/lib-java/
  build.gradle.kts                   # +micrometer-tracing-bridge-otel, +otlp exporter
  src/main/java/com/canary/platform/lib/observability/
    CanaryMetrics.java
    CanaryMetricsAutoConfiguration.java
    CanaryHttpMetricsFilter.java     # extends/replaces existing header filter wiring
    CanaryKafkaRecordInterceptor.java
    CanaryRestateInterceptor.java
    LaneStateProbe.java
    TracingAutoConfiguration.java
  src/main/resources/META-INF/spring/
    org.springframework.boot.autoconfigure.AutoConfiguration.imports  # add new autoconfigs

services/{payment,audit,inventory,order,notification}/
  src/main/resources/application.yml          # tracing + actuator config
  (deployment templates updated via Helm chart, see below)

deploy/helm/service-chart/
  templates/deployment.yaml          # add prometheus scrape annotations + actuator port

services/alert-sink/                  # NEW
  build.gradle.kts
  Dockerfile
  src/main/java/com/canary/alertsink/
    AlertSinkApplication.java
    AlertController.java
  src/main/resources/application.yml
  src/test/java/...

deploy/kind/restate/statefulset.yaml  # set RESTATE_TRACING_ENDPOINT + scrape annotations for runtime metrics

docs/runbooks/                        # NEW
  README.md
  canary-burning-budget.md
  canary-lane-drift.md
  canary-shadow-mismatch.md
  canary-lane-stuck.md
  baseline-regression.md
  restate-invocation-failure-spike.md

tests/e2e/observability/              # NEW
  ObservabilityE2ETest.java           # asserts: metric appears, alert fires, sink logs receive
```

### Install pipeline

`deploy/kind/observability/install.sh` is extended (it already installs Prometheus/Grafana/Kiali/Jaeger). New steps appended in order:
1. `kubectl apply` Alertmanager (Deployment + Service + ConfigMap)
2. `kubectl apply` alert-sink (Deployment + Service)
3. `kubectl apply` rules ConfigMap (rules-cm.yaml)
4. `kubectl apply` dashboards ConfigMap (dashboards-cm.yaml)
5. `kubectl patch` Prometheus configmap with `prometheus-config-patch.yaml` content
6. `kubectl patch` Jaeger Deployment with OTLP-receiver enablement
7. `kubectl rollout restart` Prometheus + Jaeger to pick up config
8. `kubectl rollout status` for all new Deployments — fail loud if not Ready in 180s

A new `make observability` target in the root `Makefile` wraps the script for convenience.

## Testing

### Unit (per sub-phase)
- §1 metrics: `MeterRegistry`-based assertion that the four metrics are registered with the expected tag keys, and that lane resolution from `XCanaryContext` flows into the tag.
- §1.5 tracing: span emission test asserting `canary.lane` attribute is on the active span after the filter runs.
- alert-sink: `MockMvc` test that `POST /alerts` returns 200 and emits a structured log line.

### Integration
- Sub-phase 5.b: containerized Restate + service pair, send an invocation chain, assert that Jaeger collector received spans linked by trace-id across the hop.
- Sub-phase 5.c: applied PrometheusRule passes `promtool check rules`; alertmanager config passes `amtool check-config`.

### E2E (sub-phase 5.d)
A new test under `tests/e2e/observability/` that:
1. Brings up the kind cluster with `make all` (existing) + `make observability` (new).
2. Drives a synthetic canary failure (`canary-ctl deploy payment --image image-that-500s`).
3. Polls Prometheus until `canary:availability:ratio_5m{service="payment",lane="canary"}` drops below 0.99.
4. Polls `kubectl logs -n istio-system deploy/alert-sink` until an `ALERT_RECEIVED` line containing `CanaryAvailabilityFastBurn` appears.
5. Asserts the alert payload contains `runbook_url` matching `docs/runbooks/canary-burning-budget.md`.
6. Validates each dashboard JSON loads (HTTP 200 from Grafana's `/api/dashboards/uid/...`).

Honors the existing `feedback_e2e_inpod_probes.md` rule: no `kubectl exec ... curl` against Java pods. Use `kubectl wait --for=condition=ready` then port-forward + native fetch for each probe.

## Risks and open questions

| Risk | Mitigation |
|---|---|
| Restate Java SDK 2.7.0 doesn't propagate W3C trace-context across `ctx.call` | Verification is task 0 of sub-phase 5.b. Degrade T3 to runtime-spans-only. |
| Istio Prometheus addon's ConfigMap is opinionated; patching may not survive an addon re-install | `prometheus-config-patch.yaml` is idempotent (`kubectl patch --type=merge`); `install.sh` re-applies on every run. |
| Native histograms require Prometheus 2.40+ with the feature flag | Check addon version in install.sh; fall back to `_bucket` histograms if needed. |
| Spring Boot 4 + micrometer-tracing-bridge-otel version compat with the rest of the BOM | Check at sub-phase 5.a kickoff; if conflict, use `micrometer-tracing-bridge-brave` + zipkin reporter as fallback (Jaeger accepts both protocols). |
| Cardinality explosion if `canary_shadow_mismatch_total{field=...}` has unbounded `field` values | `field` is a fixed enum derived from the schema, not user data. Documented in metric definition. |

## Operational notes

- All five new components (Alertmanager, alert-sink, rules CM, dashboards CM, jaeger-config patch) are dev-grade single-replica with no persistence beyond Prometheus's existing 15-day TSDB.
- `RESTATE_TRACING_ENDPOINT` change requires a Restate StatefulSet rollout. Pre-flight: confirm no in-flight invocations.
- Alertmanager's webhook-only routing means downtime of `alert-sink` causes alerts to silently drop after the configured retry budget. For a dev cluster this is acceptable; in production a real receiver would have HA.

## Out-of-scope (explicit, for plan-writing clarity)

- No automation for promoting a canary based on observability state (Phase 4).
- No anomaly detection, ML-based drift, or learned baselines. All thresholds are hand-set in §2.
- No mTLS or auth between Prometheus ↔ Alertmanager ↔ alert-sink. In-cluster traffic only.
- No dashboard for `canary-ctl` itself — operator usage is observed via dashboards above (lane state, alert volume).
- No log aggregation beyond `kubectl logs`.
