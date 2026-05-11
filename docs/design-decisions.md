# Design decisions

This doc captures the *why* behind the architectural choices — alternatives that were considered and rejected, trade-offs accepted, and constraints that shaped the implementation. It's a companion to:

- [history.md](history.md) — phase-by-phase log of *what shipped when*
- [architecture.md](architecture.md) — *what it looks like* (system map, services, substrates)
- [canary-mechanics.md](canary-mechanics.md) — *how it works* (header propagation, presence-watch, lifecycle)

If you're looking for "why is X the way it is?", this is where the rationale lives.

---

## Repo + cluster shape (Phase 1)

### Single monorepo over polyrepo

Polyrepo would multiply CI/CD configuration, mesh config, and the cross-service routing story 5× and obscure the cross-cutting canary patterns that are the whole point of this reference. The monorepo also makes refactors that span platform libs + service callers atomic.

### kind over minikube

`kind` was chosen for faster startup, lighter footprint, more Linux-like networking behavior, and trivial image loading via `kind load docker-image` (no registry needed for local iteration).

### One shared Helm chart parameterized over five services

All five services have the same Kubernetes shape: `Deployment` + `Service` + `ConfigMap` + `ServiceAccount`, plus a canary `Deployment` toggled by overlay. A single parameterized chart at `deploy/helm/service-chart/` enforces consistency; per-service charts would diverge over time. The trade-off is a slightly busier values-file structure (one file per service under `deploy/helm/values/`).

### `canary-ctl` manages Deployment AND VirtualService in lockstep

Istio does **not** natively support cross-subset fallback. If the `VirtualService` permanently carried both rules and only the canary `Deployment` lifecycle were managed externally, then "header set but canary subset empty" would return `503 no_healthy_upstream` — breaking the **graceful-fallback invariant** that mid-chain services with no canary deployed must not break the chain.

Solution: `canary-ctl` is the single source of truth for the canary lifecycle, creating and removing the canary `Deployment` AND the VirtualService header-match rule together. The header rule is created last on `deploy-canary` (after the canary subset has Ready endpoints) and removed first on `rollback` (before the canary subset disappears). This makes graceful fallback a structural property, not a runtime check.

---

## Kafka presence detection (Phase 2.a)

### Pod-level watch over EndpointSlice

The natural-seeming choice — watch the canary's EndpointSlice — doesn't work cleanly here. Each service's Kubernetes `Service` selects **both** subsets (`app=<svc>` only, no version label), so its EndpointSlice mixes stable and canary endpoints. Filtering by `version` would require extra lookups per event.

Instead, `XCanaryPresenceWatcher` watches Pods with the label selector `app=<svc>,version=canary` directly. One watch stream per stable pod, observing ≤2 canary pod objects in steady state. Per-pod granularity matches the actual question ("is any canary pod Ready?") and the implementation stays O(1) on the hot path (atomic flag read per Kafka record).

### Heartbeat-fresh readiness signal over first-poll

The original Phase 2.b implementation used "first poll received within `KAFKA_HEALTH_TIMEOUT`" as the Kafka readiness signal. This had two problems:

1. **Cold-cluster deadlock.** On a freshly-deployed cluster with no traffic, a brand-new canary pod never received a poll and never became Ready — `helm install --wait` would time out after 3 minutes.
2. **K5 false-positive risk.** A `SIGSTOP`'d local consumer's `assigned=true` flag stays true even after the broker has expelled it from the group — membership-only would have missed the K5 takeover trigger.

The fix replaced the signal with **"consumer joined + heartbeat fresh"** — Java reads the kafka-clients metric `last-heartbeat-seconds-ago`; Node subscribes to the kafkajs `consumer.events.HEARTBEAT` event. Threshold defaults to **15s** (5 missed heartbeats at the default 3-second interval). This is the right liveness signal for "is the consumer's Kafka client thread alive?" — not "have we recently received a message?". K5 detection dropped from ~46s to ~31s end-to-end as a side benefit.

### Stable readiness is intentionally NOT Kafka-gated

Only canary's readiness probe includes `kafkaConsumer`. Stable uses `readinessState` only. The asymmetry is deliberate: stable doesn't need a Kafka-takeover signal (only canary does), and re-gating stable would convert Kafka outages into full-service outages — a regression with no covering acceptance scenario. The canary-only opt-in lives in `deploy/helm/values/canary-overlay.yaml` (`MANAGEMENT_ENDPOINT_HEALTH_GROUP_READINESS_INCLUDE: "readinessState,kafkaConsumer"` for Java; equivalent split in Node `/health`).

---

## Restate saga compensation (Phase 3.a)

### Full saga reversal over best-effort dead-letter

The `/api/orders` saga uses **explicit compensation handlers** called by the saga in reverse order on each step's failure — `releaseReservation` after payment failure, `refund` after notify failure. Not best-effort dead-letter; not relying on auto-expire alone. The reservation timer (below) is defense-in-depth, not the primary mechanism.

### Partial reversal on notify failure (no un-confirm operation)

When notify fails after the reservation has already been **confirmed** (step 2c), the saga refunds the payment but **leaves the reservation `confirmed`**. The reservation workflow has terminated via `confirm` — there is no parked awakeable to `release`, and the workflow does not model an "un-confirm" operation. This matches real-world saga semantics where committing inventory and reversing it are distinct operations. The Kafka observer sees `inventory.events confirmed` + `payments.events refunded` and can infer the partial-reversal end state.

### TerminalException distinction

Restate retries failed steps by default. `TerminalException` opts out — used for validation failures (`invalid request: <field>`), business-rule violations (`order already refunded`, `no charge to refund`), and races against terminal workflow state (`reservation not in confirmable state`). Plain `RuntimeException` is reserved for transient infra failures (broker hiccups, R-to-R network errors) where retry is the right answer.

### 120s reservation timer

`ReservationWorkflow.run` parks on an awakeable + 120s timer. On timer expiry → status `expired` (auto-release). This is defense-in-depth for saga crashes between `reserve` and `confirm` where the explicit `release` compensation never runs. 120s was chosen as long enough for normal saga payment + notify steps but short enough to test in opt-in e2e (R4) in under 3 minutes.

---

## Restate canary routing (Phase 3.b)

### β (variant-isolated names) over α (native Restate versioning)

Two routing models were on the table:

- **α (native)** — both subsets register the same service name; rely on Restate's deployment-version selector to pick canary. Closest to the Restate-recommended model. Would have preserved `restate invocations pause/resume <id> --deployment <new>` for redirecting in-flight invocations off a buggy canary.
- **β (variant-isolated names)** — stable registers `CheckoutSagaStable` / `ReservationWorkflowStable` / `PaymentVOStable` / `NotificationServiceStable`; canary registers the same handlers under `*Canary` names. The order-service HTTP controller picks the variant by reading the incoming `x-canary` header.

**β was chosen.** It composes cleanly with Phase 1's header-routing mental model and gives a falsifiable test invariant: *"a `*Canary` invocation cannot reach a stable handler"* — verifiable from the Restate admin API alone.

**Trade-off accepted:** β loses Restate's pause/resume primitive. `restate invocations resume` requires the resume target to expose the **same** service name as the original; with `*Stable` / `*Canary` names diverging per variant, a `*Canary` invocation cannot be resumed onto stable. Canary teardown in β has two drain modes only — graceful (wait for in-flight count to reach 0, then `deployments remove`) or emergency (`invocations cancel` per id, then `deployments remove --force`).

### Deliberate asymmetry with Phase 2 (no automatic stable-takeover for Restate)

Phase 2's Kafka path implements graceful fallback via `XCanaryPresenceWatcher` + per-message filter. **Phase 3.b does not replicate this.** The order-service HTTP controller routes by header alone; when canary is unhealthy, flagged Restate calls still POST to `*Canary` and surface as HTTP 502/503 — **observable to the client**, unlike Phase 2's silent Kafka black-hole risk that made fallback essential. Operational mitigation: standard pod readiness alarms + stop flagged traffic at the Istio VirtualService during canary outages.

---

## Observability metrics + tracing (Phase 5)

### β surgical metrics over α global lane-tagging

Two metric-shape options were considered for canary-aware observability:

- **α** — apply a global Micrometer `MeterFilter` that adds a `lane` tag to **every** meter (JVM GC, scheduler threads, HTTP client, everything).
- **β** — define four explicit canary-aware metrics (`canary_request_total`, `canary_request_duration_seconds`, `canary_lane_active`, plus a per-handler Restate variant) and emit them at the three substrate entry points only. JVM / HTTP-client metrics stay vanilla.

**β was chosen.** The lane is request-scoped (lives in `XCanaryContext`), but many meters are sampled outside any request (JVM GC, scheduler thread) — α would have forced a meaningless fallback tag like `lane=none` that doubled cardinality without adding operational value. Lane discrimination on JVM metrics is also rarely actionable — if the canary pod has a memory leak, you see it on per-pod metrics already (`pod=...-canary-...`).

### T1+T2+T3 tracing contract (no skipping Restate)

Cross-substrate trace propagation was specified as a three-layer contract — T1 (HTTP), T2 (Kafka), T3 (Restate). A γ option that capped at T1+T2 (Restate handler hops appear as disconnected sub-trees) was rejected because Restate 1.6.2 + Java SDK 2.7.0 already propagate W3C trace-context across `ctx.call(...)` natively — setting `RESTATE_TRACING_ENDPOINT` on the StatefulSet costs one line. There was no engineering reason to skip T3.

### Phase 5.c (alerts + SLOs) skipped

Phase 5.c — Alertmanager + recording rules + burn-rate SLOs + paging — was deliberately skipped for this reference repo. Two reasons:

1. **No on-call audience to page.** This is a developer-laptop reference, not a production system.
2. **Burn-rate alerts need history.** Multi-window multi-burn-rate alerts (per Google's SRE Workbook) need weeks of TSDB to compute meaningful thresholds; a 15-day dev-cluster Prometheus retention with sporadic traffic can't drive them.

The shipped dashboards + four runbooks under [runbooks/](runbooks/) are sufficient for the demonstration target.

---

## Deferred phases

Why each deferred phase is out of scope — short version. Long versions live in [history.md#future-phases](history.md#future-phases).

| Phase | Why deferred / skipped |
|---|---|
| **2.c (schema evolution)** | Today all events are plain JSON with no `schemaVersion`. A schema registry choice (Confluent / Apicurio / Karapace) and wire format (JSON / Avro / Protobuf) is a separate brainstorming session; the cheapest first slice would be a `schemaVersion` field + `XCanarySchemaFilter`, but even that has open registry-choice prerequisites. |
| **4 (CI/CD + percent-split + Argo Rollouts)** | The reference is bounded to **header-routed** canary (client opt-in). A percent-split driver (Argo Rollouts / Flagger) and the automated promotion / GitHub Actions / OPA policies that wrap it are a meaningful new system, not a small extension. |
| **5.c (on-call ergonomics)** | See above. |
