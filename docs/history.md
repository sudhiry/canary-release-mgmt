# Phase-by-phase implementation history

Project context for what shipped when and why. Each phase has a paired
spec under [`superpowers/specs/`](superpowers/specs/) and an
implementation plan under [`superpowers/plans/`](superpowers/plans/).
Newer developers should read [architecture.md](architecture.md) and
[canary-mechanics.md](canary-mechanics.md) first; this document is for
"why is X the way it is?"

## Phase 1 — Substrate + HTTP canary

Phase 1 built the working substrate (5 services in 2 stacks, deployable
to a local kind cluster) plus the simplest canary axis: HTTP traffic
gated by a single header.

Goals:

1. Working monorepo, polyglot, deployable on a developer laptop.
2. Wire services together over HTTP, Kafka, and Restate (all stable-only in Phase 1).
3. HTTP canary mechanics via Istio header routing + application-level header propagation.
4. One tool (`canary-ctl`) owning the canary lifecycle end-to-end with partial-state recovery.
5. 13 canonical e2e acceptance scenarios covering positive, negative, and lifecycle dimensions.

Phase 1 was decomposed into five sub-phases.

### Plan 1.1 — Foundation (complete)

Local infrastructure. Bootstraps the kind cluster, Istio, Strimzi/Kafka,
Restate, and observability addons. Operational targets:

| Command | What it does |
| ------------------------ | ------------------------------------------------------------------ |
| `make up`                | Bootstrap kind + Istio + Strimzi/Kafka + Restate + observability   |
| `make down`              | Delete the kind cluster                                            |
| `make status`            | Show pod state across `istio-system`, `kafka`, `restate`           |
| `make smoke-infra`       | bats infrastructure smoke (11 assertions)                          |
| `make dashboards`        | Open Kiali / Grafana / Prometheus / Jaeger port-forwards (background) |
| `make dashboards-stop`   | Stop all dashboard port-forwards                                   |
| `make dashboards-status` | Show which dashboard port-forwards are running                     |

### Plan 1.2 — Shared platform libraries (complete)

Two shared libraries propagate `x-canary: true` across HTTP, Kafka, and
Restate boundaries:

**Java side — `platform/lib-java`** (Spring Boot 4 starter):

- `XCanaryRequestFilter` — inbound servlet filter; reads `x-canary` and stores in `XCanaryContext` (ThreadLocal).
- `XCanaryRestClientInterceptor` — outbound HTTP interceptor (`ClientHttpRequestInterceptor`).
- `XCanaryKafkaProducerInterceptor` — outbound Kafka header stamper.
- `XCanaryRestateClientCustomizer` — outbound Restate metadata stamper.
- `XCanaryAutoConfiguration` — Spring Boot 4 auto-config wiring all of above.

**Node side — `platform/lib-node`** (TypeScript / pnpm workspace package):

- `xCanaryMiddleware` — Express middleware; reads `x-canary` and stores in `AsyncLocalStorage`.
- `attachXCanaryAxiosInterceptor` — outbound HTTP via axios.
- `stampXCanaryOnProducerRecord` — outbound Kafka pre-send wrapper for KafkaJS records.
- `applyXCanaryToRestateOptions` — outbound Restate per-call options helper.

### Plan 1.3.a — Domain services code (complete)

Five domain services live under `services/`. Two new shared modules carry
cross-service Restate type contracts:

- `platform/restate-defs-java` — DTOs + abstract `@Service`/`@VirtualObject`/`@Workflow` definitions.
- `platform/restate-defs-node` — TS DTOs + `restate.ServiceDefinition`-style defs.

Per-service feature flags (set false on canary pods in 1.3.b):

- `KAFKA_CONSUMERS_ENABLED` — gates `@KafkaListener` (Java) / `consumer.subscribe` (Node).
- `RESTATE_REGISTER_HANDLERS` — gates the Restate HTTP endpoint listener.

Phase 1.3.a is code only — no deployment artifacts.

### Plan 1.3.b — Deployment to kind (complete)

After Plan 1.3.a, Plan 1.3.b deploys all five services to the kind
cluster behind Istio routing. Stable-only traffic; canary lifecycle is
Plan 1.4. What this ships:

- 5 multi-stage Dockerfiles (`services/<svc>/Dockerfile`)
- 5 Strimzi `KafkaTopic` CRDs (`deploy/kafka/topics/`)
- One shared Helm chart (`deploy/helm/service-chart/`) parameterized by per-service values files
- `canary-overlay.yaml` values file (used by Plan 1.4 canary-ctl; checked in but not applied)
- 5 `DestinationRule` + 5 default-only `VirtualService` files (`deploy/routing/`)
- Istio `Gateway` + edge `VirtualService` (`/api/orders` → order-service)
- Per-service Helm post-install Job that registers handlers with Restate Admin

### Plan 1.4 — canary-ctl + traffic-cli (complete)

`canary-ctl` owns the per-service canary lifecycle (Helm release +
VirtualService header-match rule + per-service state file). `traffic-cli`
sends single requests to the edge with or without `x-canary: true`.

| Command | Effect |
|---|---|
| `canary-ctl deploy-canary <svc> <tag>` | Helm install canary release + apply VS header rule. Auto-rollback on rollout failure. |
| `canary-ctl rollback <svc>` | Header rule first, grace sleep, helm uninstall, clear state. Idempotent. |
| `canary-ctl status <svc>` | Print state, helm release, VS rule presence, drift. `--json` for machine-readable. |
| `canary-ctl reconcile <svc>` | Inspect (state × cluster) cross-product; complete deploy, finish rollback, or remove drift. |

State files live at `~/.canary-ctl/<service>.json`. Override with `--state-dir`.

### Plan 1.5.a — e2e harness foundation + S1 Baseline (complete)

The TypeScript e2e harness lives in `tests/e2e/` (workspace package
`@canary/e2e`). Uses **vitest** with a sequential single-fork pool so
cluster-mutation scenarios don't conflict. Each service stamps
`x-served-version: stable | canary` on outbound HTTP responses (via
lib-java auto-config + lib-node middleware), letting tests trivially
assert which subset handled a request.

Helpers in `tests/e2e/helpers/`:

| Helper | What it does |
|---|---|
| `canary.ts` | Shells out to `node tools/canary-ctl/bin/canary-ctl` for `deployCanary`, `rollback`, `status`, `reconcile`. |
| `traffic.ts` | `sendOrder({canary, user, sku, ...})` — single POST to `/api/orders`. |
| `subset.ts` | `assertServedVersion(headers, "stable" \| "canary")`. |
| `load.ts` | `runLoad({url, rps, durationSeconds})` — TS-native load gen, returns p50/p99 + counts. |
| `kafka-admin.ts` | kafkajs admin: consumer-group descriptions. |
| `restate-admin.ts` | axios `GET :9070/deployments` and `/services`. |

### Plan 1.5.b — 12 remaining e2e scenarios (S2–S13) (complete)

Phase 1 closed out. 13 canonical scenarios live in `tests/e2e/`. See
[operations.md#scenario-coverage](operations.md#scenario-coverage) for
the table.

## Phase 2 — Kafka canary

Phase 2 added Kafka-substrate canary mechanics. Decomposed into 2.a
(libs + RBAC) and 2.b (service integration + e2e); 2.c (schema
evolution) deferred.

### Plan 2.a — Kafka canary consumer foundation (complete)

Phase 2.a shipped the **lib code + Helm RBAC** for canary-aware Kafka
consumption. NOT yet wired into services — that landed in Plan 2.b.

**lib-java** additions:

- `XCanaryConsumerGroupIdResolver` — appends `-stable` / `-canary` to base group IDs so each subset joins its own consumer group
- `XCanaryConsumeFilter` — per-message decision (canary processes only `x-canary=true`; stable processes all non-canary plus canary-flagged when canary is absent)
- `XCanaryConsumeContext.runWith(headers, handler)` — wraps a Kafka consume callback in an `XCanaryContext` frame so outbound HTTP/Kafka/Restate calls inherit `x-canary`
- `XCanaryPresenceWatcher` — long-lived k8s watch on Pods matching `app=<svc>,version=canary`; maintains atomic `canaryReady` flag updated push-style by watch events
- `KafkaConsumerHealthIndicator` — Spring Actuator HealthIndicator that reports OUT_OF_SERVICE if no successful Kafka poll within 30s (configurable)

**lib-node** additions: equivalent set — `resolveConsumerGroupId`, `shouldProcess`, `runWithCanaryFromHeaders`, `XCanaryPresenceWatcher`, `createKafkaHealthState`.

**Helm chart**: new `Role` + `RoleBinding` granting the service's
ServiceAccount `pods` get/list/watch in its namespace. Conditional on
`.Values.canaryWatch.enabled` (default `true`).

How presence detection works: each stable pod opens a long-lived watch
on canary-version pods in its namespace. K8s pushes events as canary
deploys/rolls back/crashes — typical detection lag is <1s. Hot-path
consume filter is an O(1) atomic flag read; no per-message API calls.

### Plan 2.b — Service integration + Phase 2 e2e (complete)

Plan 2.b consumed the Plan 2.a foundation. All 5 services now resolve
their Kafka consumer group ID per subset (stable → `<svc>-stable`,
canary → `<svc>-canary`), gate each message on
`XCanaryConsumeFilter` / `shouldProcess`, propagate `x-canary` into
the consume context (so downstream HTTP/Kafka/Restate calls inherit it),
and record poll timestamps that flow into the readiness probe (Java:
actuator `kafkaConsumer` indicator in the readiness group; Node:
`/health` returns 503 when the in-memory health state reports stale).

The canary overlay (`deploy/helm/values/canary-overlay.yaml`) flips
`KAFKA_CONSUMERS_ENABLED` from `"false"` to `"true"`. Per-subset
consumer groups are created by Kafka on first poll; no new KafkaTopic
CRDs are needed.

Phase 2 acceptance scenarios K1–K5 prove the four canary rules end-to-end.
See [operations.md#scenario-coverage](operations.md#scenario-coverage).

#### Post-merge fixes (cluster verification surfaced 5 issues)

After Phase 2.b merged, a cluster verification pass uncovered several
bugs that the unit-test suite didn't catch. All five are now fixed and
merged:

- **Item 1 — Cold-cluster pre-warm.** Canary deploys deadlocked on a
  freshly-deployed cluster with no traffic. `make pre-warm` + README
  workaround. ([commit 295c3f6](https://github.com/anthropics/canary-release-mgmt/commit/295c3f6))
- **Item 2 — Java listener ordering test.** Each listener test
  asserted `recordPoll` and filter-rejection separately; nothing
  covered the *order*. Extended `filterRejectionShortCircuits` to
  assert `pollFlag` after rejection. ([commit 4e7662e](https://github.com/anthropics/canary-release-mgmt/commit/4e7662e))
- **Item 3 — `KAFKA_HEALTH_TIMEOUT_MS` env-driven.** Node services
  hardcoded the timeout; now plumbed through `config.ts` to match
  Java's `canary.kafka-health-timeout-ms`. ([commit f07bf0d](https://github.com/anthropics/canary-release-mgmt/commit/f07bf0d))
- **Item 4 — Stable readiness no longer Kafka-gated.** Phase 2.b
  lumped `kafkaConsumer` into the base `application.yml` readiness
  group, so STABLE pods couldn't become Ready on a cold cluster either
  (no producer running yet → no poll → readiness 503 forever →
  `helm install --wait` timed out). Fix: stable uses `readinessState`
  only; canary's overlay adds
  `MANAGEMENT_ENDPOINT_HEALTH_GROUP_READINESS_INCLUDE: "readinessState,kafkaConsumer"`
  for the canary-only K5 takeover behavior. Same split in Node
  `/health`. ([merge ac49e98](https://github.com/anthropics/canary-release-mgmt/commit/ac49e98))
- **Item 5 — Java services were silently NOT subscribing to Kafka.**
  Spring Boot 4.0.4's `KafkaAutoConfiguration` no longer auto-imports
  `@EnableKafka` AND no longer auto-creates `ConsumerFactory` /
  `kafkaListenerContainerFactory` beans (regression from 3.x).
  Without them, `@KafkaListener` is silently a no-op — bean is
  registered, container never starts, no consumer group ever joined.
  Existing tests use `ApplicationContextRunner` and call
  `onMessage(record)` directly, so they never started a real listener
  container and the gap was invisible. Surfaced when
  `kafka-consumer-groups.sh --list` showed only the 3 Node service
  groups, zero Java. Fix: `@EnableKafka` on each Java `*Application`
  class + `ConsumerFactory<Object, Object>` +
  `ConcurrentKafkaListenerContainerFactory` beans defined in
  `XCanaryAutoConfiguration` (with `auto-offset-reset=earliest` so
  brand-new canary groups pick up pre-warm messages). ([commits
  a40a7df](https://github.com/anthropics/canary-release-mgmt/commit/a40a7df)
  + [1e66a6d](https://github.com/anthropics/canary-release-mgmt/commit/1e66a6d))

After Items 4 + 5, `make deploy-services` succeeds on a cold cluster,
all 6 expected consumer groups appear in `kafka-consumer-groups.sh
--list`, and pre-warm orders show lag=0 on every Java + Node consumer.

- **Item 6 — Cold-cluster pre-warm fixed at root cause.** Item 1's
  `make pre-warm` was a workaround. Replaced the "first poll received"
  Kafka readiness signal with "consumer joined + heartbeat fresh"
  (Java: `last-heartbeat-seconds-ago` metric; Node: kafkajs
  `consumer.events.HEARTBEAT`). Threshold default 15s (was 30s for the
  poll-receipt timeout). K5 detection is now ~31s end-to-end (was ~46s).
  `make pre-warm` is kept as an optional e2e helper. Stable readiness
  unchanged. Old env-var/property names accepted as deprecated aliases.
  Spec: `docs/superpowers/specs/2026-05-10-canary-cold-cluster-prewarm-fix-design.md`.

#### Open finding (deferred): K1 e2e saga timeout

With Items 1–5 in place, K1's `beforeAll` (5 sequential canary deploys)
succeeds — but the test phase (`sendOrder({canary: true})` POST → saga
calls inventory + payment + notification with `x-canary: true` header)
hangs past vitest's 300s `testTimeout`. Pre-warm orders during the same
window show ~50% timeout rate, suggesting the canary saga path is
genuinely flaky under load (not an infrastructure issue — those are
now fixed). Tracked as a Phase 2 follow-up. See
[operations.md#known-issues](operations.md#known-issues).

### Phase 2.c — Schema evolution (deferred)

Phase 2.c is intentionally deferred. Today every event is plain JSON
via `objectMapper.writeValueAsString(charge)` (Java) / `JSON.stringify`
(Node), with no `schemaVersion` field, no schema registry, and no
compatibility policy. This works while every service runs the same
event class but breaks the moment a canary changes an event's shape.
The cheapest first slice: add `schemaVersion: number` to every payload,
gate consumers on it via a new `XCanarySchemaFilter`, add a K6-style
scenario where canary publishes `schemaVersion=2` and stable rejects
gracefully. A registry choice (Confluent, Apicurio, Karapace) and wire
format (JSON / Avro / Protobuf) is a separate brainstorming session.

## Phase 3 — Restate canary

Phase 3 added Restate-substrate canary mechanics. Decomposed into 3.a
(make the Phase 1 stub-saga durable on Restate) and 3.b (variant-isolated
handler dispatch for canary).

### Plan 3.a — Restate substrate completion (complete, merged 2026-05-10, 2652419)

Phase 1.5's stub `/api/orders` saga ran ad-hoc axios calls. Phase 3.a
made the saga **durable**: the order-service HTTP controller submits to
the Restate Ingress, and a `CheckoutSaga` handler running inside the
order-service pod itself drives the inventory → payment → notification
calls via Restate's durable execution machinery. The saga is still
header-aware — it inherits `x-canary` from the inbound request and
propagates it on every downstream HTTP / Kafka call. Acceptance lives
in `tests/e2e/r1-r5-restate-saga.test.ts` (R1 happy path, R2 payment
compensation, R3 notify compensation, R4 + R5 reservation-workflow
slow path — opt-in via `RUN_SLOW=1`).

### Plan 3.b — Canary handler versioning (complete, merged 2026-05-11, 0463ceb)

Two routing models were considered:

- **α (native)** — both subsets register the same service name; rely
  on Restate's deployment-version selector to pick canary. Closest to
  the Restate-recommended model and would have made
  `restate invocations pause/resume <id> --deployment <new>` work for
  redirecting in-flight invocations off a buggy canary.
- **β (variant-isolated names)** — stable registers
  `CheckoutSagaStable` / `ReservationWorkflowStable` / `PaymentVOStable`
  / `NotificationServiceStable`; canary registers the same handlers
  under `*Canary` names. The order-service HTTP controller picks the
  variant by reading the incoming `x-canary` header.

**β was chosen.** It composes cleanly with Phase 1's header-routing
mental model and gives a falsifiable test invariant ("a `*Canary`
invocation cannot reach a stable handler" — verifiable from the
Restate admin API alone), at the cost of losing Restate's
pause/resume primitive: a `*Canary` invocation **cannot** be resumed
onto stable, since Restate's pause/resume requires the resume target
to expose the **same** service name. Canary teardown in β has only
two drain modes — graceful (`deployment describe --extra`, wait for
in-flight count to reach 0, then `deployments remove`) or emergency
(`invocations cancel` per id, then `deployments remove --force`).

Variant isolation is enforced by **three independent layers**:

1. **Registration under distinct service names.** Stable + canary post
   to `/deployments` with their own per-subset URI and the discovery
   round-trip returns variant-suffixed service names.
2. **In-saga client construction picks the variant.** The
   `CheckoutSaga*` handler reads `x-canary` from its invocation
   metadata and constructs Restate clients for `*Stable` or `*Canary`
   downstreams accordingly.
3. **K8s endpoint selection.** Per-subset Services
   (`<svc>-stable` / `<svc>-canary`) give Restate variant-isolated URLs
   to dispatch against, since Restate's pods sit outside the Istio
   mesh and cannot apply DestinationRule subsetting.

Each canary handler ships one observable behavioral tweak so test
assertions are falsifiable: `Order.auditTrail` includes a per-hop
`<svc>@canary` entry, `Reservation.bufferUnits=1`, `Charge.amount`
applies a 1% discount, `NotifyResult.deliveredMessage` is suffixed
with `[via canary notifier]`. Acceptance lives in
`tests/e2e/r6-restate-canary-isolation.test.ts` (R6 — concurrent
flagged + unflagged orders maintain isolation under load) and
`tests/e2e/r7-restate-canary-deploy-lifecycle.test.ts` (R7 — both
variants register without conflict, per-subset Services have correct
selectors, in-flight isolation across canary teardown).

#### Deliberate asymmetry with Phase 2 — no automatic stable-takes-over

Phase 2's Kafka path implements graceful fallback ("if `x-canary=true`
AND canary pod NOT deployed, stable processes") via the
`XCanaryPresenceWatcher` + per-message filter. **Phase 3.b does not
replicate this.** The order-service HTTP controller routes by header
alone; when canary is unhealthy, flagged requests still POST to
`/CheckoutSagaCanary/...` and Restate either 404s or retries the dead
URL until an operator intervenes. Failure surfaces as HTTP 502/503 —
**observable to the client** (unlike Phase 2's Kafka black-hole risk
that made fallback essential). Operational mitigation: standard pod
readiness alarms + stop flagged traffic at the Istio VirtualService
during canary outages.

### Plan 3.b — open items deferred to user

R7 cluster lifecycle verification (the full deploy → drain → teardown
flow against a real kind cluster, including the graceful-vs-emergency
decision branch) was deferred to user-driven verification. The
underlying β isolation invariants are exercised by R6 + the
admin-API-only checks in R7.

## Phase 5 — Observability

Phase 5 added the per-lane observability surface (metrics, traces,
dashboards, runbooks) that lets an operator answer "is the canary
worse than stable, and where?" from the dashboards alone. Decomposed
into 5.a (Java instrumentation foundation), 5.a-node (Node mirror),
5.b (trace propagation + Restate handler metric wiring), and 5.d
(focused dashboards + runbooks + an observability validator).
Phase 4 and Phase 5.c (on-call ergonomics) were skipped — no
percent-split driver and no on-call audience for this reference repo.

### Plan 5.a — Java instrumentation foundation (complete, merged 2026-05-11, e26b201)

Five `CanaryMetrics`-backed Spring beans wired by
`CanaryMetricsAutoConfiguration` emit the four canary-aware meters
on every request, every Kafka record, and every Restate handler:

- `canary_request_total` (counter; tags: `service`, `target`, `substrate`, `lane`, `outcome`)
- `canary_request_duration_seconds` (histogram; same tags)
- `canary_lane_active` (gauge; tags: `service`, `substrate`, `lane`)
- per-handler counter/histogram via `CanaryRestateMeter`

`LaneStateProbe` watches K8s endpoints for `app=<svc>,version=canary`
and emits the lane gauge. `CanaryHttpSpanFilter` tags the active span
with `canary.lane` + `canary.service`. `TracingAutoConfiguration`
strips inherited lane tags from non-canary meters (so the cardinality
explosion only happens on the canary-aware meters).

Spring Boot 4 wiring quirks discovered during 5.a:

- `CanaryMetricsAutoConfiguration` had to switch to
  `@ConditionalOnClass` (not `@ConditionalOnBean`) — Spring Boot 4's
  auto-config ordering surfaced bean-initialization races.
- Pod template gained Prometheus scrape annotations
  (`prometheus.io/scrape: "true"` + path `/actuator/prometheus`).
- Service scope: 5.a was Java-only. Node services landed in 5.a-node.

### Plan 5.a-node — Node observability instrumentation (complete, merged 2026-05-11, 3f30eae)

Mirror of 5.a for the Node side, exported from
`platform/lib-node/src/observability/`:

- `CanaryMetrics` — central helper for the four meters, backed by `prom-client`.
- `canaryHttpMetricsMiddleware` — Express middleware that times each request per-lane. Registered **inside `setupHttp`** so it fires before routes (regression caught and fixed in [f4eb977](https://github.com/anthropics/canary-release-mgmt/commit/f4eb977)).
- `wrapKafkaConsumer` — times `eachMessage` per-lane.
- `measureRestate` — handler-level metric emission helper.
- `canaryMetricsEndpoint` — exposes the prom registry at `/actuator/prometheus`.
- `initTracing` — OTel NodeSDK + auto-instrumentations + adds `canary.lane` as a span attribute.
- `LaneStateProbe` — emits `canary_lane_active` for Node services.

Both `order-service` and `notification-service` were wired with OTel
tracing + canary metrics + lane gauge.

### Plan 5.b — trace propagation + Restate handler metric wiring (complete, merged 2026-05-11, 3362564)

Closed the gaps between 5.a's per-bean instrumentation and an
end-to-end trace from edge → Kafka → Restate → handler:

- Restate runtime tracing turned on, OTLP'd to Jaeger.
- Spring Kafka observation enabled on the listener container factory + every `KafkaTemplate`, so Kafka producer + consumer spans appear in the same trace as the originating HTTP request.
- Every Java Restate handler wrapped with `CanaryRestateMeter` (PaymentVO, ReservationWorkflow, AuditQueryService).
- Every Node Restate handler wrapped with `measureRestate` (order-service, notification-service).

### Plan 5.d — focused dashboards + runbooks + observability validator (complete, merged 2026-05-11, 514951f)

Three Grafana dashboards (JSON sources under
`deploy/kind/observability/dashboards/`, applied via the Grafana
sidecar ConfigMap mechanism — `grafana_dashboard: "1"` label):

- **Canary — Overview** (uid `canary-overview`) — lane-active matrix, error rate + p95 latency by service × lane.
- **Canary — Substrates** (uid `canary-substrates`) — per-substrate (http / kafka / restate) request rate, error rate, duration heatmap, top-10 slowest targets.
- **Canary — Traces** (uid `canary-traces`) — Jaeger trace search filtered by `service` + `lane`.

`deploy/kind/observability/install.sh` applies the dashboards
ConfigMap on every install. Four runbooks under
[`docs/runbooks/`](runbooks/) cover the incident classes the
dashboards surface — canary burning budget, canary lane drift,
canary lane stuck, Restate invocation failure spike.

Acceptance scenario O1 (`tests/e2e/o1-observability-validator.test.ts`)
asserts: local JSON parses with matching uid + title, Grafana serves
each dashboard by uid, Prometheus has the three canary meters present
with `lane=canary` samples, and Jaeger has at least one trace tagged
`canary.lane=canary`.

### Phase 4 + Phase 5.c — skipped

Phase 4 (CI/CD + percent-split + Argo Rollouts) and Phase 5.c
(on-call ergonomics — alert rules, paging, SLOs) were deliberately
skipped for this reference repo: no percent-split driver is in scope
(header-routed canary is bounded by clients), and there is no on-call
audience to page. The shipped runbooks + dashboards are sufficient
for a developer-laptop demonstration.

## Future phases

| Phase | Focus | Status |
|---|---|---|
| 2.c | Schema evolution (`schemaVersion` field, registry choice, `XCanarySchemaFilter`, K6-style scenario) | Deferred |
| 4 | CI/CD, percent-split routing, automated promotion (Argo Rollouts or Flagger), GitHub Actions, OPA/Kyverno policies, canary-ctl as a controller | Skipped (no percent-split driver in scope) |
| 5.c | On-call ergonomics — alert rules, SLOs, paging | Skipped (no on-call audience) |

Phase 1 (HTTP), Phase 2.a + 2.b (Kafka), Phase 3.a + 3.b (Restate β),
and Phase 5.a + 5.a-node + 5.b + 5.d (observability) are all merged
and feature-complete on a developer laptop.
