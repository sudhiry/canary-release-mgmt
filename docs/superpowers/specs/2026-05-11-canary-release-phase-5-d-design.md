# Canary Release Phase 5.d — Dashboards + Runbooks + Observability Validator — Design

**Status:** approved (brainstorming → spec)
**Date:** 2026-05-11
**Predecessors:**
- Phase 5.a (Java foundation, merged 2026-05-11, `e26b201`)
- Phase 5.a-node (Node foundation, merged 2026-05-11, `3f30eae`)
- Phase 5.b (trace propagation + handler metric wiring, merged 2026-05-11, `3362564`)
**Parent spec:** `docs/superpowers/specs/2026-05-11-canary-release-phase-5-observability-design.md` — §2 (Grafana dashboards) and §3 (runbooks) are the design surface this sub-phase delivers.

## Why 5.c was skipped

Phase 5.c originally bundled recording rules + SLOs + Alertmanager + an alert-sink. The user asked whether 5.c is necessary before we proceed to 5.d. Findings that drove the skip:

1. **No alert audience.** This is a single-developer dev cluster. Pages with no on-call to receive them are noise.
2. **Burn-rate alerts need history.** Multi-window burn-rate calculations (1h/6h/24h) are meaningful only after weeks of clean signal, which we won't accumulate inside dev runs.
3. **Recording rules are dashboard-perf optimization, not correctness.** Dashboards in 5.d query Prometheus directly. With single-replica scrapes and a small metric set, raw queries are fast enough.
4. **Shadow-mismatch alert lost its premise.** 5.b dropped `recordShadowMismatch` wiring after discovering Phase 2/3 implemented header-routed lane isolation, not shadow reads. The signal that motivated half the alerting story doesn't exist.
5. **Runbooks attach to dashboards, not just alerts.** A runbook can be triggered by an operator noticing a spike on the overview dashboard. The "alert → runbook" path is one trigger; "dashboard → runbook" is another, and it's the realistic one for this stack today.

Skipping 5.c does not change the parent Phase 5 contract — every metric and trace 5.c would have alerted on is still emitted by 5.a/5.a-node/5.b and is queryable in Prometheus. A future Phase 5.c can be added when there's a real on-call rotation to point alerts at.

## Goal

After 5.d, an operator working a canary release can:
- Open a single Grafana dashboard and see at-a-glance whether canary lanes are healthy across HTTP, Kafka, and Restate substrates for every service.
- Click into a substrate dashboard to compare canary vs stable latency and error rate.
- Click into a traces dashboard to find a connected end-to-end trace for any in-flight canary request.
- Open a runbook in `docs/runbooks/` and follow concrete steps when one of four common incidents shows up on the dashboards.
- Run a CI-friendly validator that asserts the dashboard JSONs parse cleanly, the dashboards load against the live Grafana, and the underlying Prometheus + Jaeger queries return non-empty data when canary traffic is flowing.

## Non-goals

- **Recording rules, SLOs, alerting, Alertmanager, alert-sink** — explicitly skipped (5.c).
- **Shadow-mismatch panels** — `canary_shadow_mismatch_total` is never incremented in this codebase. No panel.
- **Per-handler dashboards beyond the substrate split** — operators can drill in via the variable selector, no need for a hand-rolled panel per handler.
- **Auto-provisioning Grafana datasources** — Istio's Grafana addon already wires Prometheus and Jaeger datasources during install. We piggyback on those.
- **Cross-cluster aggregation** — single dev cluster.
- **Replacing the existing E2E test suite** — the validator is additive and observability-focused.

## What ships

### 1. Grafana dashboard JSONs (3)

All three dashboards live under `deploy/kind/observability/dashboards/` and load into Grafana via the sidecar configmap mechanism (see §3).

| File | Purpose | Key panels |
|---|---|---|
| `canary-overview.json` | One-screen status across all 5 services and 3 substrates. The "is anything on fire" view. | Stat panel: lane-active matrix (substrate × service × lane). Time-series: error-rate per service (canary vs stable, stacked by substrate). Time-series: p95 latency per service (canary vs stable). |
| `canary-substrates.json` | Drill-in for a chosen substrate — HTTP, Kafka, Restate. Variable: `substrate`. | Time-series: request rate per (service, lane). Time-series: error rate per (service, lane). Heatmap: request duration. Top-N: slowest `target` over the chosen window. |
| `canary-traces.json` | Jaeger embed + trace search filters keyed off `canary.lane`. Variable: `lane`, `service`. | Jaeger Search panel filtering on `canary.lane=$lane` and `service.name=$service`. Linked-row panel showing recent traces. |

Each JSON conforms to Grafana 11.x dashboard schema (the version Istio's Grafana addon ships). UID is stable so dashboards survive ConfigMap reloads.

### 2. ConfigMap-based loading

Grafana in the Istio addon uses the standard `sidecar.dashboards.enabled=true` + `sidecar.dashboards.label=grafana_dashboard` pattern. We ship one ConfigMap manifest at `deploy/kind/observability/canary-dashboards-cm.yaml` that:

- Has label `grafana_dashboard: "1"` so the sidecar discovers it
- Embeds the three dashboard JSONs verbatim
- Lives in `istio-system` namespace alongside Grafana

`install.sh` is extended to apply this ConfigMap after the Istio addons are Ready. `install.sh`'s loop is unchanged; we add a single `kubectl apply -f deploy/kind/observability/canary-dashboards-cm.yaml` after the rollout-status loop.

### 3. Runbooks (4)

Plain markdown under `docs/runbooks/` (new directory). Each runbook has the same shape:

```
# <Incident name>
## Symptom
What you'd see on a dashboard or in logs that triggers this runbook.
## Likely causes
Bulleted list of the 2-4 root causes we've seen or designed against.
## Diagnosis
Step-by-step Prometheus queries, Jaeger searches, and `kubectl` commands to narrow it down.
## Mitigation
Two or three concrete actions, ordered by reversibility (least invasive first).
## Resolution / postmortem hooks
What to commit to the postmortem doc + any follow-up tickets.
```

The four runbooks:

| File | Triggering symptom |
|---|---|
| `docs/runbooks/canary-burning-budget.md` | Canary lane error rate or latency clearly worse than stable on the overview dashboard. |
| `docs/runbooks/canary-lane-drift.md` | `canary_lane_active` shows a substrate flipped from canary→0 unexpectedly, or stable→0. |
| `docs/runbooks/canary-lane-stuck.md` | Both lanes show traffic but `canary-ctl` reports the rollout is past its bake window without a promotion. |
| `docs/runbooks/restate-invocation-failure-spike.md` | `canary_request_total{substrate="restate", outcome!="success"}` spikes for one or more handlers. |

These four cover the realistic incidents the canary system can produce: burn (the canary is bad), drift (deploy lane state is inconsistent), stuck (operator workflow failure), and Restate-specific (the substrate most likely to surface latent state-machine bugs).

### 4. Lightweight observability validator

`tests/e2e/o1-observability-validator.test.ts` — a single vitest file that runs in the existing `tests/e2e/` harness and validates:

- All three dashboard JSONs parse as valid JSON and have matching `uid` + `title` fields.
- After applying the ConfigMap, Grafana's `/api/dashboards/uid/<uid>` returns 200 for each dashboard (Grafana sidecar picked them up).
- A burst of canary traffic is generated via the existing `helpers/load.ts` + `helpers/canary.ts` utilities.
- Prometheus `/api/v1/query` returns a non-empty result for each of the four metrics with at least one `lane="canary"` series.
- Jaeger's `/api/traces?tags={"canary.lane":"canary"}` returns at least one trace with the expected service spans linked.

This is a dev-cluster smoke check, not a load test. It runs in <2 minutes and uses port-forward helpers we already have.

## Architecture

### Where dashboard JSONs live

```
deploy/kind/observability/
├── install.sh                          # extended: apply dashboards CM
├── dashboards/                         # NEW
│   ├── canary-overview.json            # NEW
│   ├── canary-substrates.json          # NEW
│   └── canary-traces.json              # NEW
└── canary-dashboards-cm.yaml           # NEW (envelopes the 3 JSONs)
```

The ConfigMap is generated by hand (not by `kustomize` or `helm`), so the YAML file embeds the JSON content as multi-line strings. We accept the duplication between `dashboards/*.json` (source of truth, easy to edit) and the inline copies in the ConfigMap because the CM is regenerated by a script step in `install.sh` rather than being hand-edited.

Specifically: `install.sh` reads the three JSONs, generates the ConfigMap on the fly with `kubectl create configmap --from-file=... --dry-run=client -o yaml | kubectl apply -f -`, and labels it for the Grafana sidecar. The static CM YAML in the repo is for reference + editor diff context only — it is regenerated by install.

### Dashboard variables

Both `canary-substrates.json` and `canary-traces.json` use templated variables:

| Variable | Source | Default |
|---|---|---|
| `substrate` | Static list: `http`, `kafka`, `restate` | `http` |
| `service` | Prometheus `label_values(canary_request_total, service)` | `All` |
| `lane` | Static list: `stable`, `canary`, `All` | `All` |

`canary-overview.json` does NOT take variables — it's the default landing page.

### Panel queries (sketch)

Error rate by lane, per service:
```promql
sum by (service, lane) (
  rate(canary_request_total{outcome!~"success|2..|3..", substrate=~"$substrate"}[$__rate_interval])
)
/
sum by (service, lane) (
  rate(canary_request_total{substrate=~"$substrate"}[$__rate_interval])
)
```

p95 latency by lane:
```promql
histogram_quantile(0.95,
  sum by (service, lane, le) (
    rate(canary_request_duration_seconds_bucket{substrate=~"$substrate"}[$__rate_interval])
  )
)
```

Lane active matrix:
```promql
canary_lane_active
```

Restate handler error spike:
```promql
sum by (target, lane) (
  rate(canary_request_total{substrate="restate", outcome!="success"}[5m])
)
> 0
```

### Validator wiring

The validator reuses existing helpers:
- `helpers/cluster.ts` for clean baseline
- `helpers/canary.ts` to flip a service to canary so we generate canary-tagged traffic
- `helpers/load.ts` to drive burst HTTP traffic
- `helpers/pod-port-forward.ts` to reach Prometheus, Grafana, and Jaeger from the test process

A new tiny helper `helpers/observability.ts` adds:
- `queryPrometheus(query: string): Promise<PromVector>`
- `getGrafanaDashboard(uid: string): Promise<unknown>`
- `searchJaegerTraces(opts: {service: string; tags: Record<string,string>; limit?: number}): Promise<JaegerTrace[]>`

All three port-forward Grafana / Prometheus / Jaeger via `pod-port-forward.ts`. No new image dependency, no new RBAC.

## Scope discipline

**In scope (must ship in this PR):**
- 3 dashboard JSONs at `deploy/kind/observability/dashboards/`
- `canary-dashboards-cm.yaml` reference + script-driven CM apply in `install.sh`
- 4 runbook markdown files at `docs/runbooks/`
- 1 validator vitest at `tests/e2e/o1-observability-validator.test.ts`
- 1 helper file at `tests/e2e/helpers/observability.ts`
- README pointer from `docs/onboarding.md` (or wherever the existing operations doc lives) to the new runbooks dir — small docs polish, no spec change.

**Out of scope (explicitly):**
- Recording rules, alerting rules, Alertmanager, alert-sink — see §"Why 5.c was skipped"
- Shadow-mismatch panels — metric never incremented today
- Replacing or deleting any existing E2E test
- Cross-cluster federation
- Per-handler dashboards beyond the `target` variable drill-in
- Grafana datasource provisioning — Istio addon already wires it

**Cluster verification:** Like prior 5.* phases, end-to-end on-cluster verification of the validator is deferred to the user. Local file validation (JSON parses, vitest TypeScript compiles) is in scope.

## Risks

| Risk | Mitigation |
|---|---|
| Grafana 11.x dashboard schema differs subtly from the version Istio's addon ships at the user's pinned `ISTIO_VERSION` | Validator hits Grafana's own `/api/dashboards/uid/...` endpoint after CM load — schema mismatch surfaces as 4xx. Document supported schema in each JSON's `__inputs`. |
| ConfigMap regeneration in `install.sh` adds a non-trivial bash step | Keep the regeneration to one `kubectl create configmap ... --dry-run=client -o yaml` line. The static `canary-dashboards-cm.yaml` checked in is for reviewers, not consumed by install. |
| Validator port-forwarding adds flakiness | Reuse existing `pod-port-forward.ts` (already battle-tested across r1-r7 and s1-s13 tests). Add timeouts + retry. |
| Runbooks rot when metric names change | Each runbook embeds the actual PromQL it expects. Validator will fail on metric rename, surfacing the doc drift. |
| Jaeger trace search by tag may not find traces if SDK propagation fell through to manual fallback (5.b Tasks 8/9) | Validator searches by `canary.lane` AND falls back to filtering by `service.name`-only if the tag search returns empty. The fallback path logs a warning so the user knows propagation may need investigation. |

## Operational notes

- Dashboards live in the `istio-system` namespace alongside Grafana; the sidecar discovers any CM with `grafana_dashboard: "1"` regardless of namespace, so the namespace choice is purely organizational.
- No new container ports.
- No RBAC changes — the test harness already has the read access it needs.
- Runbooks are markdown — no rendering pipeline; they're read in editor or on GitHub.
- After 5.d, the parent Phase 5 spec is satisfied modulo the explicitly-deferred 5.c. We can close out the Phase 5 epic.

## Out-of-scope (explicit)

- No changes to application code (no service `index.ts`, no `lib-java`, no `lib-node`).
- No changes to Helm charts or service manifests.
- No new env vars.
- No changes to Phase 1/2/3/4 routing semantics or to Phase 5.a/5.b emission contracts.
