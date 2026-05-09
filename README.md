# canary-release-mgmt

A reference architecture for canary release management across HTTP, Kafka,
and Restate.dev in a polyglot microservice system.

This repository is built in phases. See `docs/superpowers/specs/` for design
specs and `docs/superpowers/plans/` for implementation plans.

## Phase 1 — Substrate + HTTP canary

Quickstart:

    make up         # bootstrap kind + Istio + Kafka + Restate
    make smoke-infra # verify all infra is Ready
    make down       # tear down

Full design: `docs/superpowers/specs/2026-05-08-canary-release-phase-1-design.md`

## Plan 1.1 — Foundation (complete)

Local infrastructure is in place. The following commands are operational:

| Command                  | What it does                                                       |
| ------------------------ | ------------------------------------------------------------------ |
| `make up`                | Bootstrap kind + Istio + Strimzi/Kafka + Restate + observability   |
| `make down`              | Delete the kind cluster                                            |
| `make status`            | Show pod state across `istio-system`, `kafka`, `restate`           |
| `make smoke-infra`       | Run bats infrastructure smoke tests (11 assertions)                |
| `make dashboards`        | Open Kiali / Grafana / Prometheus / Jaeger port-forwards (background) |
| `make dashboards-stop`   | Stop all dashboard port-forwards                                   |
| `make dashboards-status` | Show which dashboard port-forwards are running                     |
| `make verify`            | Run all Java + Node unit tests (~118 tests)                        |
| `make build-services`    | Compile all 5 service binaries (Java bootJars + Node tsc dist)     |
| `make help`              | List available targets                                             |

The Kafka cluster is reachable inside the mesh at
`my-cluster-kafka-bootstrap.kafka:9092`. Restate's admin API is reachable
inside the mesh at `restate.restate:9070` and from the host on
`http://localhost:9070`. Istio's ingress gateway is reachable from the host
on `http://localhost:8080`.

Observability dashboards (run `make dashboards` to open all four in the background, or port-forward individually):
- Kiali:      `kubectl -n istio-system port-forward svc/kiali 20001:20001` → http://localhost:20001
- Grafana:    `kubectl -n istio-system port-forward svc/grafana 3000:3000` → http://localhost:3000
- Prometheus: `kubectl -n istio-system port-forward svc/prometheus 9090:9090` → http://localhost:9090
- Jaeger:     `kubectl -n istio-system port-forward svc/tracing 16686:80` → http://localhost:16686

Stop the port-forwards with `make dashboards-stop` (or `kill` the PIDs printed by `make dashboards`).

Next phase: 1.2 (shared platform libraries for `x-canary` propagation).

## Plan 1.2 — Shared platform libraries (complete)

Two shared libraries propagate `x-canary: true` across HTTP, Kafka, and Restate boundaries:

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

Build / test:

| Command       | What it does                                       |
| ------------- | -------------------------------------------------- |
| `make verify` | Run all Java + Node unit tests (~36 tests total)   |
| `./gradlew :platform:lib-java:test` | Java side only                |
| `pnpm --filter @canary/lib-node test` | Node side only              |

## Plan 1.3.a — Domain services code (complete)

Five domain services live under `services/`:

| Service | Stack | HTTP port (local) | Restate port | Kafka producer | Kafka consumer |
|---|---|---|---|---|---|
| order-service | TS + Node | 3001 | 9084 | `orders.events` | `payments.events`, `inventory.events` |
| payment-service | Java + Spring Boot | 8081 | 9081 | `payments.events` | `orders.events` |
| inventory-service | Java + Spring Boot | 8082 | 9082 | `inventory.events` | `orders.events` |
| notification-service | TS + Node | 3002 | 9085 | `notifications.events` | `orders.events`, `payments.events` |
| audit-service | Java + Spring Boot | 8083 | 9083 | `audit.events` | all `*.events` |

Two new shared modules carry cross-service Restate type contracts:
- `platform/restate-defs-java` — DTOs + abstract `@Service`/`@VirtualObject`/`@Workflow` definitions.
- `platform/restate-defs-node` — TS DTOs + `restate.ServiceDefinition`-style defs.

Per-service feature flags (set false on canary pods in 1.3.b):
- `KAFKA_CONSUMERS_ENABLED` — gates `@KafkaListener` (Java) / `consumer.subscribe` (Node).
- `RESTATE_REGISTER_HANDLERS` — gates the Restate HTTP endpoint listener.

Build / test:

| Command | What it does |
|---|---|
| `make verify` | Run all Java + Node unit tests (~118 tests) |
| `make build-services` | Compile all 5 service binaries (Java bootJars + Node `tsc` dist) |
| `./gradlew :services:<name>:test` | Java service tests in isolation |
| `pnpm --filter @canary/<name> test` | Node service tests in isolation |

**Phase 1.3.a is code only — no deployment artifacts.** Dockerfiles, KafkaTopic CRDs, k8s manifests, image build scripts, and the canary Helm overlay are 1.3.b.

## Plan 1.3.b — Deployment to kind (complete)

After Plan 1.3.a (services compile + tests green), Plan 1.3.b deploys all five services to the kind cluster behind Istio routing. Stable-only traffic; canary lifecycle is Plan 1.4.

### Quickstart

```bash
make up                  # bootstrap cluster + Istio + Kafka + Restate (1.1)
make build-services      # compile Java + Node (1.3.a)
make build-images        # docker build all 5 service images
make load-images         # kind load image cache
make deploy-services     # KafkaTopics + Helm install + Istio routing
make smoke-services      # bats smoke test
```

### What this ships

- 5 multi-stage Dockerfiles (`services/<svc>/Dockerfile`)
- 5 Strimzi `KafkaTopic` CRDs (`deploy/kafka/topics/`)
- One shared Helm chart (`deploy/helm/service-chart/`) parameterized by per-service values files (`deploy/helm/values/<svc>.yaml`)
- `canary-overlay.yaml` values file (used by Plan 1.4 canary-ctl; checked in but not applied)
- 5 `DestinationRule` + 5 default-only `VirtualService` files (`deploy/routing/`)
- Istio `Gateway` + edge `VirtualService` (`/api/orders` → order-service)
- Per-service Helm post-install Job that registers handlers with Restate Admin

### Service inventory

| Service | Stack | HTTP port | Restate port | Probes |
|---|---|---|---|---|
| audit-service | Java + Spring Boot | 8083 | 9083 | /actuator/health/{liveness,readiness} |
| payment-service | Java + Spring Boot | 8081 | 9081 | /actuator/health/{liveness,readiness} |
| inventory-service | Java + Spring Boot | 8082 | 9082 | /actuator/health/{liveness,readiness} |
| order-service | TypeScript + Node | 3001 | 9084 | /health |
| notification-service | TypeScript + Node | 3002 | 9085 | /health |

### Verifying

```bash
make smoke-services      # 5 bats assertions, ~60s
kubectl get -n services pods,svc,deploy
kubectl get -n services destinationrules,virtualservices
helm list -n services    # should show 5 releases, all deployed
curl -s http://localhost:9070/deployments | jq '.deployments | length'  # 5
curl -s -X POST -H 'content-type: application/json' \
  -d '{"userId":"u1","sku":"sku-1","quantity":1,"amount":100}' \
  http://localhost:8080/api/orders
```

### Tearing down (without destroying the cluster)

```bash
make undeploy-services
```

Next phase: 1.4 (canary-ctl + per-service VirtualService header rule for `x-canary: true`).

## Plan 1.4 — canary-ctl + traffic-cli (complete)

`canary-ctl` owns the per-service canary lifecycle (Helm release + VirtualService header-match rule + per-service state file). `traffic-cli` sends single requests to the edge with or without `x-canary: true`.

### Quickstart

```bash
make up                                      # 1.1
make build-services                          # 1.3.a
make build-images && make load-images        # 1.3.b
make deploy-services                         # 1.3.b

# 1.4 commands:
make canary-deploy SVC=payment-service TAG=dev    # creates payment-service-canary release + adds header rule
make canary-status SVC=payment-service            # show state, helm release, VS rule, drift
node tools/traffic-cli/bin/traffic-cli order --canary
make canary-rollback SVC=payment-service          # remove header rule, drain, uninstall, clear state
make smoke-canary                                 # bats test (~3 minutes against real cluster)
```

### canary-ctl commands

| Command | Effect |
|---|---|
| `canary-ctl deploy-canary <svc> <tag>` | Helm install canary release + apply VS header rule. Auto-rollback on rollout failure. |
| `canary-ctl rollback <svc>` | Header rule first, grace sleep, helm uninstall, clear state. Idempotent. |
| `canary-ctl status <svc>` | Print state, helm release, VS rule presence, drift. `--json` for machine-readable. |
| `canary-ctl reconcile <svc>` | Inspect (state × cluster) cross-product; complete deploy, finish rollback, or remove drift. |

State files live at `~/.canary-ctl/<service>.json`. Override with `--state-dir`.

### traffic-cli

```bash
traffic-cli order [--canary] [--user u1] [--sku sku-1] [--quantity 1] [--amount 100] [--url http://localhost:8080]
```

Sends one POST to the kind ingress. `--canary` adds `x-canary: true`. Verifying which subset *served* the request belongs to Plan 1.5's e2e harness — for 1.4 use Kiali (http://localhost:20001) to confirm by eye.

Next phase: 1.5 (13 canonical acceptance scenarios).

## Plan 1.5.a — e2e harness foundation + S1 Baseline (complete)

The TypeScript e2e harness lives in `tests/e2e/` (workspace package `@canary/e2e`). It uses **vitest** with a sequential single-fork pool so cluster-mutation scenarios don't conflict. Each service stamps `x-served-version: stable | canary` on outbound HTTP responses (via lib-java auto-config + lib-node middleware), letting tests trivially assert which subset handled a request.

### Quickstart

```bash
make up                                                   # 1.1
make build-services                                       # 1.3.a + new lib changes
make build-images && make load-images                     # 1.3.b
make deploy-services                                      # 1.3.b

# 1.5.a additions:
make e2e SCENARIO=s1                                      # run S1 Baseline only
make e2e                                                  # run all e2e scenarios (just S1 in 1.5.a)
make ci-local                                             # fast subset (just S1 in 1.5.a)
```

### What S1 verifies

S1 Baseline asserts that on a clean stable cluster (no canary deployed):
- `POST /api/orders` without `x-canary` returns 2xx with `x-served-version: stable`
- `POST /api/orders` with `x-canary: true` ALSO returns 2xx with `x-served-version: stable` (graceful fallback)

This doubles as coverage for the umbrella spec's S5 (no-canary graceful fallback). S5 still gets its own dedicated file when 1.5.b ships, for clarity.

### Helpers

`tests/e2e/helpers/` contains reusable building blocks for 1.5.b's scenarios:

| Helper | What it does |
|---|---|
| `canary.ts` | Shells out to `node tools/canary-ctl/bin/canary-ctl` for `deployCanary`, `rollback`, `status`, `reconcile`. |
| `traffic.ts` | `sendOrder({canary, user, sku, ...})` — single POST to `/api/orders`. |
| `subset.ts` | `assertServedVersion(headers, "stable" \| "canary")`. |
| `load.ts` | `runLoad({url, rps, durationSeconds})` — TS-native load gen, returns p50/p99 + counts. |
| `kafka-admin.ts` | kafkajs admin: consumer-group descriptions. (Used by S10 in 1.5.b.) |
| `restate-admin.ts` | axios `GET :9070/deployments` and `/services`. (Used by S11 in 1.5.b.) |

Next phase: 1.5.b (12 remaining scenarios S2–S13).

## Plan 1.5.b — 12 remaining e2e scenarios (S2–S13) (complete)

Phase 1 is now complete. All 13 canonical acceptance scenarios from the umbrella spec are implemented as separate vitest files in `tests/e2e/`.

### Quickstart

```bash
make up && make build-services && make build-images && make load-images && make deploy-services
make e2e                                                  # run all 13 (~15 min)
make ci-local                                             # fast subset: S1, S2, S5, S8, S9, S12 (~5 min)
make e2e SCENARIO=s7                                      # run one
```

### Scenario coverage

| # | Name | What it asserts |
|---|---|---|
| S1 | Baseline | All-stable cluster: no-header AND header request both 2xx + `x-served-version: stable` |
| S2 | Single-service canary | Canary on payment → chain shows `payment-service=canary`, others stable |
| S3 | Multi-service canary | Canary on order + inventory → both `=canary`, others stable |
| S4 | Full-chain canary | Canary on all 5 → every chain entry `=canary` |
| S5 | No-canary fallback | Header request with no canary → stable serves |
| S6 | Canary unhealthy | Bad image tag → auto-rollback fires; final state clean |
| S7 | Stable undisrupted | p99 stable load during canary deploy ≤ 1.5× baseline |
| S8 | Header propagation completeness | Chain contains all 5 services (every internal hop reached) |
| S9 | Header leak prevention | No-header request: no canary pod logs the user ID |
| S10 | Kafka isolation | Canary pods don't join Kafka consumer groups |
| S11 | Restate isolation | Canary pods don't register with Restate Admin |
| S12 | Rollback | Deploy + rollback → cluster fully clean |
| S13 | Partial-state recovery | Manual VS rule deletion → `canary-ctl reconcile` repairs |

### Per-hop chain (`x-served-chain` header)

Each service stamps `<svc>=<version>` and prepends downstream service tokens captured via the axios/RestClient response interceptor. Tests parse the comma-separated chain to verify multi-hop routing without needing Jaeger.

Phase 1 is complete. Next: Phase 2 (Kafka canary consumer strategies).

## Plan 2.a — Kafka canary consumer foundation (complete)

Phase 2.a ships the **lib code + Helm RBAC** for canary-aware Kafka consumption. NOT yet wired into services — that lands in Plan 2.b. After 2.a merges, the cluster's behavior is unchanged; the new lib code sits unused until 2.b consumes it.

### What landed

**lib-java** (`platform/lib-java/`):
- `XCanaryConsumerGroupIdResolver` — appends `-stable` / `-canary` to base group IDs so each subset joins its own consumer group
- `XCanaryConsumeFilter` — per-message decision: canary processes only `x-canary=true`; stable processes all non-canary plus canary-flagged when canary is absent (graceful fallback)
- `XCanaryConsumeContext.runWith(headers, handler)` — wraps a Kafka consume callback in an `XCanaryContext` frame so outbound HTTP/Kafka/Restate calls inherit `x-canary`
- `XCanaryPresenceWatcher` — opens a long-lived k8s watch on `Pods` matching `app=<svc>,version=canary`; maintains an atomic `canaryReady` flag updated push-style by watch events
- `KafkaConsumerHealthIndicator` — Spring Actuator HealthIndicator that reports OUT_OF_SERVICE if no successful Kafka poll within 30s (configurable)

**lib-node** (`platform/lib-node/`): equivalent set — `resolveConsumerGroupId`, `shouldProcess`, `runWithCanaryFromHeaders`, `XCanaryPresenceWatcher`, `createKafkaHealthState`.

**Helm chart** (`deploy/helm/service-chart/`): new `Role` + `RoleBinding` granting the service's ServiceAccount `pods` get/list/watch in its namespace. Conditional on `.Values.canaryWatch.enabled` (default `true`).

### How presence detection works

Each stable pod opens a long-lived watch on canary-version pods in its namespace. K8s pushes events as canary deploys/rolls back/crashes — typical detection lag is <1s. Hot-path consume filter is an O(1) atomic flag read; no per-message API calls.

The canary pod's readiness probe is gated on Kafka consumer health (Plan 2.b adds the wiring to call `recordPoll()` after each successful consume). When the canary's consumer disconnects, the readiness probe fails → kubelet drops the pod from the EndpointSlice → stable's pod watch sees the pod transition to `Ready=False` → stable's flag flips → stable processes the next canary-flagged event.

### Operator smoke check (after deploy)

```bash
# RBAC works:
kubectl auth can-i watch pods -n services --as=system:serviceaccount:services:payment-service
# Expected: yes

# HealthIndicator surfaces (Java services with Actuator):
kubectl -n services exec deploy/payment-service-stable -- curl -s localhost:8081/actuator/health | jq '.components | keys'
# Expected: includes "kafkaConsumer" (Spring auto-discovers HealthIndicator beans by class name → camelCase)
```

Next: Plan 2.b wires these abstractions into all 5 services, flips `KAFKA_CONSUMERS_ENABLED=true` in canary-overlay, and adds Phase 2 e2e scenarios K1–K5.

## Plan 2.b — Service integration + Phase 2 e2e (complete)

Plan 2.b consumes the Plan 2.a foundation. All 5 services now resolve their Kafka consumer group ID per subset (stable → `<svc>-stable`, canary → `<svc>-canary`), gate each message on `XCanaryConsumeFilter` / `shouldProcess`, propagate `x-canary` into the consume context (so downstream HTTP/Kafka/Restate calls inherit it), and record poll timestamps that flow into the readiness probe (Java: actuator `kafkaConsumer` indicator in the readiness group; Node: `/health` returns 503 when the in-memory health state reports stale).

The canary overlay (`deploy/helm/values/canary-overlay.yaml`) flips `KAFKA_CONSUMERS_ENABLED` from `"false"` to `"true"`. Per-subset consumer groups are created by Kafka on first poll; no new KafkaTopic CRDs are needed.

Phase 2 acceptance scenarios K1–K5 (under `tests/e2e/`) prove the four canary rules end-to-end:

- **K1** — canary deployed + flagged event → only canary's `consumedEventStore` records it
- **K2** — canary deployed + unflagged event → only stable's store records it
- **K3** — canary NOT deployed + flagged event → stable's store records it (graceful fallback)
- **K4** — flagged event consumed by canary's audit-service → downstream Kafka events at canary-side consumers carry `x-canary: true`
- **K5** — canary process SIGSTOP'd → readiness probe fails → stable's pod watch flips → stable processes the next flagged event

Subset-aware verification uses `kubectl port-forward pod/<name>` to each subset's pod (via `tests/e2e/helpers/pod-port-forward.ts`) and queries `/internal/consumed-events` directly — the edge gateway only routes `/api/orders`, and Istio subset-by-header is in-mesh-only, so the test runner has to address pods individually.

Phase 2 (Kafka canary) is now feature-complete. Schema evolution (Phase 2.c) is deferred.
