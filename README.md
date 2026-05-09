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

Next phase: 1.3.b (deployment artifacts so the services run on the kind cluster from Plan 1.1).
