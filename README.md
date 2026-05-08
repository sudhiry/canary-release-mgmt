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
