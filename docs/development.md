# Development guide

Local setup, build, test, and run cycle for `canary-release-mgmt`. New
developers should read [architecture.md](architecture.md) first for the
big picture.

## Prerequisites

Install before cloning:

| Tool | Required version | Install |
|---|---|---|
| Docker | any recent | Docker Desktop / Colima / Rancher Desktop |
| `kind` | latest | `brew install kind` |
| `kubectl` | matching cluster (1.29+) | `brew install kubectl` |
| `helm` | 3.x | `brew install helm` |
| `istioctl` | 1.29.2 (matches `ISTIO_VERSION` in [Makefile](../Makefile)) | https://istio.io/latest/docs/setup/getting-started/#download |
| Java | JDK 25 | sdkman / brew |
| Node | 20+ | nvm / brew |
| pnpm | 9.12.0 (pinned in `package.json`) | `corepack enable && corepack prepare pnpm@9.12.0 --activate` |
| `bats` | latest | `brew install bats-core` |
| `jq` | latest | `brew install jq` |

The Java toolchain is pinned to JDK 25 in [build.gradle.kts](../build.gradle.kts).
Spring Boot 4.0.4 requires Java 17+; 25 is what we test against.

## First-time setup

```bash
git clone <repo-url>
cd canary-release-mgmt

# Install workspace dependencies (pnpm + gradle handle the rest on first build)
pnpm install
./gradlew tasks   # downloads gradle distribution + warms cache
```

## Verify you can run the unit-test suite

This is the fastest "is my machine set up?" check. No cluster needed.

```bash
make verify
# Runs ./gradlew test (Java side, ~80 tests)
# Plus pnpm -r --filter "@canary/*" run test (Node side, ~80 tests)
```

If both halves pass, your toolchain is ready.

## The Makefile is your primary entrypoint

Every workflow has a target. Run `make help` to see all of them. Cheat
sheet:

| Target | What it does |
|---|---|
| `make up` | Bootstrap kind + Istio + Strimzi/Kafka + Restate + observability addons |
| `make down` | Delete the kind cluster |
| `make status` | Pod state across `istio-system`, `kafka`, `restate`, `services` |
| `make smoke-infra` | bats infrastructure smoke (post-`up`) |
| `make verify` | All Java + Node unit tests |
| `make build-services` | Compile all 5 service binaries (Java bootJars + Node `tsc dist`) |
| `make build-images` / `make load-images` / `make images` | Docker build + kind load |
| `make deploy-services` | KafkaTopics + Helm install all 5 + Istio routing |
| `make pre-warm` | Send 3 baseline orders (optional; useful before e2e suites that measure consumer lag) |
| `make smoke-services` | bats deploy smoke |
| `make canary-deploy SVC=<s> TAG=<t>` | Deploy a canary via canary-ctl |
| `make canary-rollback SVC=<s>` | Roll a canary back |
| `make canary-status SVC=<s>` | Show canary state, helm release, VS rule, drift |
| `make canary-reconcile SVC=<s>` | Repair drift between state file and cluster |
| `make smoke-canary` | bats canary-ctl smoke (~3 min, real cluster) |
| `make e2e [SCENARIO=s7]` | All 18 e2e scenarios, or a single one |
| `make ci-local` | Fast subset (S1, S2, S5, S8, S9, S12) for inner-loop iteration |
| `make dashboards` / `dashboards-stop` / `dashboards-status` | Port-forwards for Kiali / Grafana / Prometheus / Jaeger |
| `make undeploy-services` | Tear down services without nuking the cluster |

The standard inner-loop:

```bash
# After editing service code:
make build-services && make build-images && make load-images
kubectl -n services rollout restart deployment <svc>     # picks up the new image
make smoke-services
```

## Running a single service locally (no cluster)

Java services use Spring Boot's `bootRun`:

```bash
./gradlew :services:payment-service:bootRun
# or
SPRING_PROFILES_ACTIVE=local ./gradlew :services:payment-service:bootRun
```

Node services use the workspace package's `start` script (after building):

```bash
pnpm --filter @canary/order-service build
pnpm --filter @canary/order-service start
```

For local-only development you'll need to set the environment yourself
(see `services/order-service/src/config.ts` for defaults). Most of the
canary plumbing fails closed when its dependencies are unreachable, so
running a service in isolation works as long as you set
`KAFKA_PRODUCER_ENABLED=false`, `KAFKA_CONSUMERS_ENABLED=false`, and
`RESTATE_REGISTER_HANDLERS=false`.

## Environment variables

The [Helm chart](../deploy/helm/service-chart/templates/configmap.yaml)
plus per-service [values](../deploy/helm/values/) are the source of truth.
Key knobs every service honors:

| Variable | Default | Purpose |
|---|---|---|
| `VERSION` | `stable` | Set to `canary` ONLY by `canary-overlay.yaml`. Drives subset routing, consumer-group suffix, presence-watcher mode, response stamping. |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` (cluster: `my-cluster-kafka-bootstrap.kafka:9092`) | Kafka brokers |
| `KAFKA_CONSUMERS_ENABLED` | `true` | Gates `@KafkaListener` (Java) / `consumer.subscribe` (Node) |
| `KAFKA_PRODUCER_ENABLED` | `true` | Gates the producer wiring |
| `KAFKA_HEARTBEAT_STALE_MS` (Node) / `canary.kafka-heartbeat-stale-ms` (Java) | `15000` | After this many ms with no consumer heartbeat, the health indicator goes OUT_OF_SERVICE. Old `*-health-timeout-ms` names accepted as deprecated aliases. |
| `MANAGEMENT_ENDPOINT_HEALTH_GROUP_READINESS_INCLUDE` | `readinessState` (stable); `readinessState,kafkaConsumer` (canary, via overlay) | Spring Actuator readiness group composition. The `kafkaConsumer` gate is canary-only by design — only canary needs the takeover signal; stable doesn't, and re-gating it would convert Kafka outages into full-service outages. |
| `RESTATE_REGISTER_HANDLERS` | `true` for both stable and canary (since Phase 3.b) | Gates the in-pod Restate endpoint listener. Both subsets register handlers under variant-suffixed service names (`*Stable` / `*Canary`). Set `false` only for local-dev runs without a Restate server. |
| `RESTATE_INGRESS_URL` | `http://localhost:9070` (cluster: `http://restate.restate:9070`) | Restate admin |
| `POD_NAMESPACE` | `services` | Where the presence watcher looks for canary pods |
| `SERVICE_NAME` | (per service) | Stamped into `x-served-chain` and used by the presence watcher selector |
| `OTLP_TRACING_ENDPOINT` | `http://jaeger-collector.istio-system:4317` | OTLP gRPC endpoint for traces (Phase 5.b). Both stacks honor this. |

## Running tests

### Unit tests

Run by stack or project:

```bash
make verify                                   # all of them
./gradlew :platform:lib-java:test             # Java lib only
./gradlew :services:payment-service:test      # one Java service
pnpm --filter @canary/lib-node test           # Node lib only
pnpm --filter @canary/order-service test      # one Node service
pnpm --filter @canary/canary-ctl test         # canary-ctl
```

### bats smoke tests

These hit a real cluster and require the relevant `make` targets to have
been run first.

```bash
make smoke-infra      # post-`make up`
make smoke-services   # post-`make deploy-services`
make smoke-canary     # post-`make deploy-services`; ~3 min
```

### End-to-end scenarios

27 scenarios across four families, each in its own file under
[tests/e2e/](../tests/e2e/). Vitest is configured for a single
sequential fork pool because every scenario mutates cluster state.

| Family | Files | Phase |
|---|---|---|
| HTTP | S1–S13 | 1.5 |
| Kafka | K1–K6 (K6 opt-in via `RUN_COLD_CLUSTER_TESTS=true`) | 2.b |
| Restate | R1–R7 (R4/R5 opt-in via `RUN_SLOW=1`; R7 cluster-lifecycle opt-in) | 3.a + 3.b |
| Observability | O1 (local + cluster smoke) | 5.d |

```bash
make e2e                      # all (~20 min)
make e2e SCENARIO=s7          # single scenario
make e2e SCENARIO=r6          # Restate isolation
make e2e SCENARIO=o1          # observability validator
make ci-local                 # fast subset: S1, S2, S5, S8, S9, S12 — ~5 min
```

A scenario's `beforeAll` typically deploys canaries on a clean substrate;
its `afterAll` rolls them back. If a run is interrupted, `make canary-reconcile
SVC=<svc>` repairs the state. See the K1 known-issue note in
[operations.md](operations.md#known-issues).

## Known Spring Boot 4 quirks

If you change the Java listener wiring, three traps are worth knowing:

1. **`@EnableKafka` is mandatory on each `*Application` class.** Spring Boot
   4.x's `KafkaAutoConfiguration` no longer auto-imports it. Without it,
   `@KafkaListener` is silently a no-op — bean registers, container never
   starts, no consumer group ever joined. We had this regression in Phase 2.b;
   `kafka-consumer-groups.sh --list` showed only the Node groups.
2. **`ConsumerFactory` and `kafkaListenerContainerFactory` beans are
   provided by `XCanaryAutoConfiguration`.** Spring Boot 4.0.4 stopped
   auto-creating both. We construct them with `auto.offset.reset=earliest`
   so a brand-new `<svc>-canary` consumer group can pick up any traffic
   produced before it joined.
3. **Stable's readiness group must NOT include `kafkaConsumer`.** See
   `application.yml` and `canary-overlay.yaml`. Stable doesn't need a
   Kafka takeover signal — only canary does — and keeping the gate
   canary-only avoids any cold-cluster surprise on stable.

Canary readiness uses consumer heartbeat freshness
(`last-heartbeat-seconds-ago` metric), not message receipt. A brand-new
canary pod becomes Ready as soon as its consumer joins the group and
emits a heartbeat — typically <5s after pod start, even on a cluster
with zero traffic.

These three rules are interlocking — if you change the Java Kafka wiring
or the readiness configuration, run `make smoke-canary` AND eyeball
`kubectl -n kafka exec my-cluster-kafka-0 -- bin/kafka-consumer-groups.sh
--bootstrap-server localhost:9092 --list` to confirm all 6 expected groups
appear (`<svc>-stable` × 5 + `audit-service-canary` × 1 once you deploy
that canary, plus per-deploy `<svc>-canary` groups).

## Observability (Phase 5)

Both stacks emit canary-aware metrics + traces out of the box once a
service imports the platform library. Surface:

- Metrics on `/actuator/prometheus` (both stacks). Pods carry
  `prometheus.io/scrape: "true"` so the in-cluster Prometheus picks
  them up automatically.
- Traces via OTLP gRPC to `OTLP_TRACING_ENDPOINT` (default
  `jaeger-collector.istio-system:4317`). Java wires this in
  `application.yml`; Node wires it via `initTracing(...)` called
  during service bootstrap.
- The four meters (`canary_request_total`,
  `canary_request_duration_seconds`, `canary_lane_active`, plus a
  per-handler counter/histogram pair) are tagged with `service`,
  `target`, `substrate`, `lane`, `outcome`.
- Spans are tagged with `canary.lane` + `canary.service`.

When editing the Java side, the auto-config wiring lives in
`platform/lib-java/src/main/java/com/canary/platform/lib/observability/`
(`CanaryMetricsAutoConfiguration`, `TracingAutoConfiguration`). The
Node side is opt-in per-service — see
`platform/lib-node/src/observability/` for the export surface and
`services/order-service/src/http.ts` for canonical wiring.

The canary dashboards live in
`deploy/kind/observability/dashboards/`. After editing a JSON,
re-apply with `ISTIO_VERSION=$ISTIO_VERSION bash deploy/kind/observability/install.sh`.

## Build artifacts

| Artifact | Where | Built by |
|---|---|---|
| Java bootJars | `services/<svc>/build/libs/<svc>-0.0.1-SNAPSHOT.jar` | `./gradlew :services:<svc>:bootJar` |
| Node `dist/` | `services/<svc>/dist/` | `pnpm --filter @canary/<svc> build` |
| Docker images | local Docker daemon, tag `canary-release-mgmt/<svc>:dev` | `make build-images` |
| kind image cache | inside the cluster | `make load-images` (after build) |

## IDE setup

- **Java**: import the root as a Gradle project. `./gradlew` is checked in;
  IntelliJ + VS Code "Extension Pack for Java" both work out of the box.
- **TypeScript**: open the workspace at the root. pnpm workspaces are
  auto-detected by VS Code. Run `pnpm install` once to populate
  `node_modules` so TS imports resolve.

## Ergonomic tips

- Use `make canary-status SVC=<s>` after every `canary-deploy` /
  `rollback` / scenario run — it's the fastest way to see drift.
- The dashboards (`make dashboards`) are essential for diagnosing routing
  issues; Kiali shows the per-subset traffic split visually.
- After modifying anything in `platform/lib-java` or `platform/lib-node`,
  rebuild the consuming services — workspace deps are linked but bootJars
  / compiled `dist/` are not regenerated automatically.
