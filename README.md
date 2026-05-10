# canary-release-mgmt

A reference architecture for canary release management across HTTP, Kafka,
and Restate.dev in a polyglot microservice system. Five domain services in
two stacks (TypeScript + Node, Java + Spring Boot 4) deploy to a local
**kind** cluster behind Istio, exchange events through Kafka, and register
durable handlers with Restate. A single `x-canary: true` HTTP header drives
canary routing on every substrate.

> **What's the point?** This repo answers the question *"how do you canary
> a polyglot, event-driven, durably-orchestrated microservice system without
> harming stable releases?"* — concretely, on a developer laptop, with the
> same mechanics used in production: Istio header-based routing, per-subset
> Kafka consumer groups, and a presence-watch protocol that lets stable
> take over when canary becomes unhealthy.

Phase 1 (HTTP canary) and Phase 2 (Kafka canary) are merged. Phase 2.c
(schema evolution), Phase 3 (Restate canary), Phase 4 (CI/CD + percent-split
\+ Argo Rollouts), and Phase 5 (observability polish) are future work.

## TL;DR — first 10 minutes

```bash
# Prereqs: docker, kind, kubectl, helm, istioctl 1.29.2, JDK 25, node 20+,
# pnpm 9.12.0, bats, jq. Full list in docs/development.md.

git clone <repo-url> && cd canary-release-mgmt
pnpm install                                    # workspace deps

make verify                                     # all unit tests, ~118 tests, no cluster needed

make up                                         # ~4 min — kind + Istio + Kafka + Restate
make smoke-infra                                # 11 assertions

make build-services && make build-images && make load-images
make deploy-services                            # Helm install all 5 + Istio routing
make smoke-services

make pre-warm                                   # ⚠ REQUIRED before first canary on a cold cluster

# Send a baseline (no header) order:
node tools/traffic-cli/bin/traffic-cli order

# Deploy a canary, send a flagged request, roll back:
make canary-deploy SVC=payment-service TAG=dev
node tools/traffic-cli/bin/traffic-cli order --canary
make canary-status SVC=payment-service
make canary-rollback SVC=payment-service

# When done:
make down                                       # destroys the cluster
```

## Documentation

For a new developer, read in this order:

| Document | What's in it |
|---|---|
| [docs/architecture.md](docs/architecture.md) | System map, the 5 services, repo layout, lib-java vs lib-node primitives |
| [docs/canary-mechanics.md](docs/canary-mechanics.md) | How `x-canary` propagates, per-subset Kafka groups, presence-watch protocol, `canary-ctl` lifecycle |
| [docs/development.md](docs/development.md) | Prereqs, build, test, run a service in isolation, env vars, Spring Boot 4 quirks |
| [docs/operations.md](docs/operations.md) | Cold-cluster bring-up, dashboards, e2e scenarios (S1–S13 + K1–K5), troubleshooting, known issues |
| [docs/history.md](docs/history.md) | Phase-by-phase implementation log + post-merge fixes |
| [docs/superpowers/specs/](docs/superpowers/specs/) | Design specs (one per phase / sub-phase) |
| [docs/superpowers/plans/](docs/superpowers/plans/) | Implementation plans (one per phase / sub-phase) |

If you only have time for one: **[docs/canary-mechanics.md](docs/canary-mechanics.md)** —
that's where the interesting bits live.

## What's in the box

```
canary-release-mgmt/
├── Makefile                  # primary entrypoint — every workflow has a target
├── deploy/                   # kind, Helm chart + values, Istio routing, KafkaTopics
├── platform/
│   ├── lib-java/             # Spring Boot 4 starter — filters, interceptors, watchers, health
│   ├── lib-node/             # TS package — middleware, axios + KafkaJS interceptors
│   └── restate-defs-{java,node}/  # cross-service Restate type contracts
├── services/                 # 5 domain services (3 Java + 2 Node)
├── tools/
│   ├── canary-ctl/           # per-service canary lifecycle CLI
│   └── traffic-cli/          # send a single /api/orders POST with/without x-canary
├── tests/
│   ├── infra/ services/ canary/   # bats smoke tests
│   └── e2e/                       # 13 HTTP + 5 Kafka scenarios (vitest)
└── docs/                     # architecture, mechanics, development, operations, history
```

## Common workflows

| Goal | Command |
|---|---|
| Set up a fresh laptop | `pnpm install && make verify` |
| Bring up the substrate | `make up && make smoke-infra` |
| Build + deploy all services | `make build-services && make build-images && make load-images && make deploy-services && make pre-warm` |
| Run all unit tests | `make verify` |
| Run all e2e scenarios | `make e2e` (~15 min) |
| Run the fast inner-loop e2e subset | `make ci-local` (~5 min, S1+S2+S5+S8+S9+S12) |
| Deploy a canary | `make canary-deploy SVC=<svc> TAG=<tag>` |
| Inspect canary state | `make canary-status SVC=<svc>` |
| Roll a canary back | `make canary-rollback SVC=<svc>` |
| Repair canary drift | `make canary-reconcile SVC=<svc>` |
| Open observability dashboards | `make dashboards` (Kiali / Grafana / Prometheus / Jaeger) |
| Tear down | `make down` |

`make help` lists every target.

## Substrate versions

Pinned at the top of [Makefile](Makefile):

- Istio 1.29.2
- Strimzi 0.45.2 (Kafka via the Strimzi operator)
- Restate 1.6.2 (server) — wire-compatible with Java SDK 2.7.0 + Node SDK 1.14.2
- Spring Boot 4.0.4 / JDK 25
- pnpm 9.12.0 / Node 20+

Don't bump the Restate server pin without testing both SDKs.

## Known issues

- **Cold-cluster pre-warm.** First `canary-deploy` on a fresh cluster will
  deadlock unless you run `make pre-warm` after `make deploy-services`.
  Canary readiness is gated on Kafka health, which only flips UP after the
  first delivered message. Pre-warm sends 3 baseline orders to seed every
  consumer.
- **K1 e2e saga timeout (deferred).** K1's flagged-saga path hangs past 5
  min on a real cluster; unit tests still pass. Tracked as a Phase 2
  follow-up — see [docs/operations.md#known-issues](docs/operations.md#known-issues).
- **Phase 2.c, Phase 3 deferred.** Schema evolution and Restate canary
  handler versioning are explicitly out of scope today. See
  [docs/history.md](docs/history.md) for the deferral rationale.

## Contributing

The conventional flow is:

1. Read [docs/development.md](docs/development.md) for environment setup
   and the Spring Boot 4 quirks.
2. Make the change. `make verify` should pass.
3. If you touched the Helm chart, deploy scripts, or the canary lifecycle,
   run `make smoke-canary` and at least `make ci-local`.
4. If you touched anything in the Kafka path, eyeball
   `kubectl -n kafka exec my-cluster-kafka-0 -- bin/kafka-consumer-groups.sh
   --bootstrap-server localhost:9092 --list` to confirm the expected groups
   appear.

## License

[MIT](LICENSE) © 2026 Sudhir Yelikar.
