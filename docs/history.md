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

## Future phases

| Phase | Focus |
|---|---|
| 2.c | Schema evolution (deferred) |
| 3 | Restate canary handler versioning + durable-execution safety |
| 4 | CI/CD, percent-split routing, automated promotion (Argo Rollouts or Flagger), GitHub Actions, OPA/Kyverno policies, canary-ctl as a controller |
| 5 | Observability polish (Grafana dashboards, alerting, runbooks, SLOs) |

Phase 2 (Kafka canary) is feature-complete on the routing + readiness
axes. Schema evolution → Phase 2.c. Restate canary handler versioning
→ Phase 3. Argo Rollouts / percent-split → Phase 4.
