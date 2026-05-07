# Canary Release Management — Phase 1 Design

**Status:** Approved (awaiting user review)
**Date:** 2026-05-08
**Phase:** 1 of 5

## Project context

The overall project is a production-ready reference architecture demonstrating canary release management for a polyglot, event-driven, durably-orchestrated microservice system. The reference covers five services that communicate over HTTP, exchange events through Kafka, and register handlers with Restate.dev. The driving question is: *how can we release canary versions of these services without harming stable releases across the HTTP, Kafka, and Restate axes?*

The work is decomposed into five phases. Each phase has its own design, plan, and implementation cycle.

| Phase | Focus |
|---|---|
| **1 (this spec)** | Substrate (5 services + HTTP + Kafka + Restate, all stable) and HTTP canary mechanics |
| 2 | Kafka canary strategies (consumer groups, header-routed events, schema evolution) |
| 3 | Restate canary handler versioning and durable-execution safety |
| 4 | CI/CD, percent-split routing, automated promotion (Argo Rollouts or Flagger), GitHub Actions, OPA/Kyverno policies, canary-ctl as a controller |
| 5 | Observability polish (Grafana dashboards, alerting, runbooks, SLOs) |

Phase 1's contribution is the working substrate and the simplest canary axis: HTTP traffic gated by a single header (`x-canary: true`).

## Goals

1. Build a working monorepo with five services in two stacks, deployable to a local Kubernetes cluster on a developer laptop.
2. Wire the services together over HTTP, Kafka, and Restate. All three substrates run in stable-only mode in Phase 1.
3. Implement HTTP canary release mechanics using Istio header-based routing and application-level header propagation.
4. Provide a single tool (`canary-ctl`) that owns the canary lifecycle end to end, with partial-state recovery.
5. Ship 13 canonical end-to-end acceptance scenarios that prove the canary system behaves correctly across positive, negative, and lifecycle dimensions.

## Non-goals (Phase 1)

The following are explicitly out of Phase 1 scope and are deferred to the listed later phase:

- Percent-split traffic routing (Phase 4)
- Kafka canary consumer strategies, including separate consumer groups, header-routed events, and schema versioning (Phase 2)
- Restate canary handler versioning, deployment selection on invocations, and durable-execution safety with concurrent registrations (Phase 3)
- Automated metric-driven promotion or abort (Phase 4)
- GitHub Actions CI for build, push, and test (Phase 4)
- Real cloud cluster deploys to EKS, GKE, or AKS (currently out of project scope)
- Production observability polish, dashboards, alerts, runbooks, and SLOs (Phase 5)
- OPA or Kyverno admission policies for label hardening (Phase 4)
- Conversion of `canary-ctl` from a CLI script with a state file into a Kubernetes controller (Phase 4)
- Schema registry or event schema evolution (Phase 2)

## Routing model (locked)

Phase 1's canary trigger is a single HTTP header: **`x-canary: true`**.

- **Origin.** The header is set at the edge by a test client, beta-flag system, or API gateway on the initial request. There is no percent split in Phase 1.
- **Propagation.** Every service that receives a request carrying `x-canary: true` must forward the header on every downstream call: HTTP, Kafka producer, and Restate handler invocation. Header propagation is the application's responsibility, implemented by the shared platform libraries described below. The pattern is identical to tracing or baggage propagation.
- **Per-target resolution.** When a request arrives at a service, that service's `VirtualService` rules decide:
  - `x-canary: true` AND a canary subset has endpoints → route to canary.
  - Otherwise → route to stable.
- **Independent per-service lifecycle.** Each service can have a canary deployed or not, independently of others. A header-flagged request flowing through `A → B → C` will hit canary-A if deployed (else stable-A), then canary-B if deployed (else stable-B), then canary-C if deployed (else stable-C).
- **Graceful fallback.** "Header set but no canary deployed for this service" must fall back to stable, not return 503. This is non-negotiable because mid-chain services with no canary deployed must not break the chain. The mechanism is described in the data flow section.
- **Without the header, traffic is always stable.** Even if a canary is healthy and serving, non-flagged users never see it.

## High-level architecture

**Repo.** Single monorepo. Polyrepo would multiply CI/CD, mesh config, and the cross-service routing story 5×, and obscure the cross-cutting canary patterns that are the point of the reference.

**Cluster.** Local **kind** cluster, chosen over minikube for faster startup, lighter footprint, more Linux-like behavior, and easier image loading via `kind load docker-image`.

**Service mesh.** Istio in default profile. For each service:

- One `Deployment` for stable, labels include `version: stable`. Always present.
- One `Deployment` for canary, labels include `version: canary`. Present only while a canary is active.
- One `Service` selecting both versions on `app: <service-name>`.
- One `DestinationRule` defining `stable` and `canary` subsets by version label.
- One `VirtualService` with two `http` rules: rule 1 matches `headers.x-canary: "true"` and routes to the `canary` subset; rule 2 is the default and routes to `stable`. **The header rule is created and removed by `canary-ctl` in lockstep with the canary Deployment**, ensuring graceful fallback when no canary is deployed.

**Header propagation.** Application-level. Two shared platform libraries — `lib-java` for Spring Boot and `lib-node` for TypeScript on Node — provide:

- An inbound filter or middleware that reads `x-canary` from the request and stores it in request-scoped context (Spring `RequestContextHolder` / scoped bean for Java; `AsyncLocalStorage` for Node).
- HTTP client interceptors (Spring `RestClient`/`WebClient`; axios for Node) that copy the context flag onto outbound requests.
- Kafka producer interceptors that add `x-canary` to outbound message headers.
- Restate client interceptors that add `x-canary` to handler invocation metadata.

**Edge.** Istio Ingress Gateway. A test harness (`tools/traffic-cli`) sends requests with and without the header.

**Observability.** Prometheus, Grafana, and Kiali — the standard Istio observability stack — plus distributed tracing via OpenTelemetry to Tempo or Jaeger. Dashboards split metrics by `version` label so canary and stable behavior can be compared. Polish is deferred to Phase 5.

**Image flow.** Local only in Phase 1. `docker build` produces images tagged `canary-release-mgmt/<service>:dev`. `kind load docker-image` puts them in the cluster's image cache. Deployments use `imagePullPolicy: IfNotPresent`. No registry, no GitHub Actions, no remote anything in Phase 1; CI is added in Phase 4.

## Components and repo layout

```
canary-release-mgmt/
├── services/
│   ├── order-service/          # TS + Node, checkout entrypoint
│   ├── payment-service/        # Java + Spring Boot, charges
│   ├── inventory-service/      # Java + Spring Boot, stock reservations
│   ├── notification-service/   # TS + Node, notifications
│   └── audit-service/          # Java + Spring Boot, append-only event log
├── platform/
│   ├── lib-java/               # Spring starter: filter + HTTP/Kafka/Restate interceptors
│   └── lib-node/               # TS package: AsyncLocalStorage middleware + interceptors
├── deploy/
│   ├── kind/                   # cluster config + bootstrap (Istio, Strimzi, Restate)
│   ├── helm/
│   │   ├── service-chart/      # one shared chart, parameterized for all 5 services
│   │   └── values/
│   │       ├── order-service.yaml
│   │       ├── payment-service.yaml
│   │       ├── inventory-service.yaml
│   │       ├── notification-service.yaml
│   │       ├── audit-service.yaml
│   │       └── canary-overlay.yaml   # values overlay that adds the canary Deployment
│   └── routing/
│       ├── destination-rules/  # one DestinationRule per service
│       └── virtual-services/   # one VirtualService per service (default rule only; canary rule managed by canary-ctl)
├── tools/
│   ├── traffic-cli/            # CLI to send test requests with/without x-canary
│   └── canary-ctl/             # CLI: deploy-canary, rollback, reconcile, status
├── docs/
│   └── superpowers/specs/      # spec docs (this file lives here)
├── tests/
│   ├── e2e/                    # 13 acceptance scenarios (TypeScript)
│   └── integration/            # per-service integration tests
└── Makefile                    # up, down, build, load, deploy, e2e, ci-local
```

### Why one shared Helm chart

All five services have the same Kubernetes shape: `Deployment` + `Service` + `ConfigMap` + `ServiceAccount`, plus a canary `Deployment` toggled by overlay. A single parameterized chart enforces consistency and is the right idiom for a reference architecture; per-service charts would diverge over time.

### `lib-java` (Spring starter)

- `XCanaryRequestFilter` — inbound filter; reads `x-canary` from the request and stores it in `XCanaryContext`, a request-scoped bean.
- `XCanaryRestClientInterceptor` — outbound HTTP interceptor registered on all `RestClient`/`WebClient` beans; copies the flag from context onto outbound requests.
- `XCanaryKafkaInterceptor` — Kafka producer interceptor; copies the flag onto outbound message headers. (Phase 1 produces but does not consume on this header; Phase 2 will route consumption based on it.)
- `XCanaryRestateInterceptor` — Restate client interceptor; copies the flag onto invocation metadata.

### `lib-node` (TypeScript)

- `xCanaryMiddleware` — Express/Fastify middleware reading inbound; stores in `AsyncLocalStorage`.
- `xCanaryAxiosInterceptor` — request interceptor on the shared axios instance; copies the flag onto outbound requests.
- Equivalent Kafka producer and Restate client interceptors.

### Domain code

Kept deliberately thin. Each service has 1–3 endpoints and a small in-pod store (H2 for Java; in-memory or sqlite for Node). Phase 1 is about routing and propagation; the e-commerce domain shapes the call graph but does not require realistic depth. Domain logic is a vehicle for the canary scenarios, not the artifact.

### Service inventory and call graph

| Service | Stack | HTTP surface | Kafka role | Restate role |
|---|---|---|---|---|
| `order-service` | TS + Node | `POST /api/orders`, `GET /api/orders/{id}` | produces `orders.events`; consumes `payments.events`, `inventory.events` | registers `CheckoutSaga` workflow; invokes `PaymentVO` and `ReservationWorkflow` |
| `payment-service` | Java + Spring Boot | `POST /charges`, `GET /charges/{id}` | produces `payments.events`; consumes `orders.events` | registers `PaymentVO` virtual object (idempotent per orderId); invokes audit handler |
| `inventory-service` | Java + Spring Boot | `POST /reservations`, `GET /products/{sku}/availability` | produces `inventory.events`; consumes `orders.events` | registers `ReservationWorkflow` (reserve-with-timeout); invokes audit handler |
| `notification-service` | TS + Node | `POST /notifications`, `GET /notifications/by-user/{userId}` | produces `notifications.events`; consumes `orders.events`, `payments.events` | registers `NotificationService` service; invokes audit handler |
| `audit-service` | Java + Spring Boot | `POST /audit/events`, `GET /audit/by-aggregate/{id}` | produces `audit.events` (checkpoints); consumes all `*.events` topics | registers `AuditQueryService`; terminal — invokes no others |

**Sync HTTP call graph (the canary header propagation surface):**

```
edge → order-service.POST /api/orders
         ├─→ inventory-service.POST /reservations
         │     └─→ audit-service.POST /audit/events
         ├─→ payment-service.POST /charges
         │     └─→ audit-service.POST /audit/events
         └─→ notification-service.POST /notifications
               └─→ audit-service.POST /audit/events
```

Edge → 3-deep chain → audit. Every service has inbound traffic that can be canary-tested.

### Phase 1 substrate simplifications for Kafka and Restate

Because Kafka consumer canaries and Restate handler-version canaries are Phase 2 and 3 work respectively, Phase 1 ships these defaults to keep them safe and out of the way:

- **Kafka.** Canary pods run with `KAFKA_CONSUMERS_ENABLED=false`. They produce events normally (events from canary pods carry `x-canary: true` Kafka headers — captured for use by Phase 2 — but are consumed by stable consumers identically to non-canary events). Canary pods do not subscribe to topics, so they cannot steal partitions from stable consumer groups.
- **Restate.** Canary pods run with `RESTATE_REGISTER_HANDLERS=false`. Stable pods alone own handler registration. Canary pods can invoke handlers as clients but do not register their own.

These flags are hard-coded in the canary Helm values overlay, and the platform libraries refuse to start a Kafka consumer or register Restate handlers when they are set false on a canary pod.

## Data flow

### Request without `x-canary` (stable path)

1. Client → ingress: `POST /api/orders`, no header.
2. Ingress evaluates `order-service`'s `VirtualService`. The header rule does not match. Default rule routes to the `stable` subset.
3. `order-service` (stable) middleware reads no `x-canary` from inbound; context flag is false.
4. `order-service` calls `inventory-service`, `payment-service`, and `notification-service` via axios. The interceptor reads context, no header is set on outbound calls.
5. Each downstream service's `VirtualService` does not see the header; default rule applies; stable subset serves.
6. Each `audit-service` call from payment, inventory, and notification is also stable.
7. Kafka events emitted by stable code carry no `x-canary` header. Stable consumers (the only consumers in Phase 1) consume normally.
8. Restate handler invocations carry no `x-canary` metadata; only stable handlers are registered.

### Request with `x-canary: true` (canary path)

1. Client → ingress: `POST /api/orders`, `x-canary: true`.
2. Ingress evaluates `order-service`'s `VirtualService`.
   - If `canary-ctl` has activated `order-service` (i.e., a canary Deployment AND the matching header rule exist), the header rule matches → route to `canary` subset.
   - If `canary-ctl` has not activated `order-service` (no canary Deployment, no header rule), the header rule is absent → default rule routes to `stable`.
3. `order-service` (whichever version served) middleware reads `x-canary: true` and stores it in context.
4. `order-service` calls `inventory-service`, `payment-service`, and `notification-service`. The outbound interceptor reads context and adds `x-canary: true` to each outbound request.
5. Each downstream service's `VirtualService`: the header rule matches if and only if `canary-ctl` has activated that service. Otherwise default rule applies; stable subset serves.
6. Each `audit-service` call carries the header. Same per-target resolution applies.
7. Kafka events produced by any pod in the canary chain carry `x-canary: true` on the message header. Phase 1 consumers ignore it; Phase 2 will use it.
8. Restate invocations include the flag in metadata. Phase 1 has only stable handlers registered.

### Multi-service partial canary

Canary deployed on `order-service` AND `inventory-service`. No canary on `payment-service`, `notification-service`, `audit-service`. Header request:

- `order-service`: header rule activated → canary order-service.
- canary order-service propagates header → inventory-service header rule activated → canary inventory-service.
- canary order-service propagates header → payment-service header rule absent → stable payment-service.
- canary order-service propagates header → notification-service header rule absent → stable notification-service.
- inventory-service (canary), payment-service (stable), notification-service (stable) each propagate header → audit-service header rule absent → stable audit-service.

Result chain: `canary-order → canary-inventory → stable-audit`, plus `canary-order → stable-payment → stable-audit`, plus `canary-order → stable-notification → stable-audit`. All transitions are clean; graceful fallback works mid-chain.

### Why `canary-ctl` manages both Deployment and VirtualService

Istio does not natively support cross-subset fallback. If the `VirtualService` permanently carried both rules and only the canary Deployment lifecycle were managed externally, then "header set but canary subset empty" would return `503 no_healthy_upstream` — breaking the graceful fallback rule. To preserve graceful fallback, `canary-ctl` is the single source of truth for the canary lifecycle, creating and removing the canary Deployment AND the VirtualService header-match rule together.

## Error handling and fallbacks

**A. Canary lifecycle failures.** `canary-ctl deploy-canary <svc> <tag>` watches rollout status with `progressDeadlineSeconds: 120`. If pods never become Ready, `canary-ctl` auto-rolls back: it removes the header rule first (no traffic exposure) and then deletes the Deployment. After Ready, if pods later crash-loop, Istio removes them from the subset; the canary subset becomes empty and header-flagged requests return 503. Phase 1 documents this as expected — testers see 503 and know the canary is broken. Auto-rollback on health regression is Phase 4 (Argo Rollouts).

**B. `canary-ctl` partial state.** A multi-step apply that fails between steps could leave a Deployment without a header rule, or vice versa. `canary-ctl` writes a state file (`~/.canary-ctl/<service>.state`) after each successful step. On startup it reconciles. Explicit `canary-ctl reconcile <svc>` command. Phase 4 may convert this into a Kubernetes controller.

**C. Subset and label contamination.** A canary pod accidentally labeled `version: stable` would silently join the stable subset and serve real traffic. `canary-ctl` is the only path to create canary Deployments, and the Helm overlay template hard-codes `version: canary`. Phase 4 adds an OPA/Kyverno policy at admission.

**D. Header propagation gaps.** A new service that does not use `lib-java`/`lib-node`, or uses a raw HTTP client without the interceptor, drops the header silently. Mitigation: unit tests in each library verifying interceptor registration; an e2e scenario asserts the header reaches all 5 services along the known call graph; Kiali traffic graph visually confirms which subsets a request hit.

**E. Accidental Kafka or Restate engagement on canary pods.** If `KAFKA_CONSUMERS_ENABLED=false` or `RESTATE_REGISTER_HANDLERS=false` is misconfigured on the canary overlay, canary pods could steal Kafka partitions or register competing Restate handlers. Mitigations: (1) the canary Helm values overlay hard-codes both flags to `false`; (2) `lib-java`/`lib-node` check the flags at startup and refuse to start consumers or register handlers on canary pods; (3) e2e scenarios verify Kafka consumer-group membership and Restate handler registry contain only stable pods.

**F. Rollback (intentional).** `canary-ctl rollback <svc>` removes the VirtualService header rule first (cuts off new canary traffic), waits a configurable grace period (default 10 s) so any in-flight canary requests can complete, then deletes the canary Deployment. Pod-level `terminationGracePeriodSeconds` and Envoy connection draining handle the final shutdown of canary pods. State file is cleared. Idempotent. The order matters — the inverse of deploy. Covered by an e2e scenario.

**G. VirtualService rule ordering.** The header-match rule must be ordered before the default rule. The Helm template enforces ordering at apply time; `canary-ctl` uses a JSON-merge patch that inserts at index 0. A negative test misorders rules in a fixture and verifies header requests fall through to stable, proving ordering matters and is enforced.

## Testing strategy

Three test layers:

1. **Unit tests** per service (business logic).
2. **Library tests** for `lib-java` and `lib-node` covering the propagation primitive itself: inbound filter reads the header, context survives async hops, outbound interceptor adds the header on HTTP, Kafka, and Restate calls. These are the highest-value tests in Phase 1 because everything else depends on the libraries being correct.
3. **End-to-end tests** running against a fresh `kind` cluster via `make up && make e2e`. Written in TypeScript for a single test-harness language across the polyglot system. Each scenario is a function callable individually: `make e2e SCENARIO=S3`.

### Phase 1 canonical acceptance scenarios

All 13 must pass.

| # | Name | Setup | Asserts |
|---|---|---|---|
| S1 | Baseline | All stable, no canaries deployed | 200s with and without header; no canary pods receive traffic (no canary pods exist) |
| S2 | Single-service canary | Canary on `payment-service` only | Header request → canary-payment, others stable; no-header request → 100% stable |
| S3 | Multi-service canary | Canary on `order` and `inventory` | Mixed chain: header → canary-order → canary-inventory → stable-audit; verified via Jaeger trace |
| S4 | Full-chain canary | Canary on all 5 | Every hop is canary with header |
| S5 | No-canary graceful fallback | All stable; send header request | 200 OK to stable (header rule absent → default rule serves) |
| S6 | Canary unhealthy | Canary configured to crash-loop | Header request → 503 within timeout; stable traffic unaffected |
| S7 | Stable not disrupted by canary deploy | Steady stable load while canary is deployed | Zero stable failures; no stable pod restarts; p99 latency within tolerance band |
| S8 | Header propagation completeness | Canary on all 5; one header request | Trace shows `x-canary: true` on 100% of internal spans across the 3-deep chain |
| S9 | Header leak prevention | Canary on all 5; one no-header request | No canary pod logs any inbound request |
| S10 | Kafka isolation | Canary on producer (`order-service`) | Events flow to topic; only stable consumer groups; canary did NOT subscribe (verified via Kafka admin API) |
| S11 | Restate isolation | Canary on `payment-service` | Restate deployment registry contains only stable; canary did NOT register |
| S12 | Rollback | Active canary, then `canary-ctl rollback` | Header rule removed; canary Deployment terminated; subsequent header requests → stable; state file cleared |
| S13 | `canary-ctl` partial-state recovery | Interrupted deploy (Deployment exists, header rule missing) | `canary-ctl reconcile` completes or rolls back; final state is consistent |

Coverage rationale: S1–S5 cover correct routing across canary topologies (positive). S6, S9, S10, S11 cover no leaks or contamination (negative). S7, S12, S13 cover deploy and rollback without disrupting stable (lifecycle). S8 covers the propagation primitive being correct.

**Load characteristics.** S1, S2, and S7 use a small load generator (Vegeta or k6) at roughly 50 rps for 30 s — enough to surface timing or race issues without burning the developer laptop.

**Continuous local verification.** `make ci-local` runs unit + library + a fast e2e subset (S1, S2, S5, S8, S9, S12) in roughly 5 minutes. Full e2e suite is roughly 15 minutes.

## Operator workflow

```
make up                                    # bootstrap kind, Istio, Strimzi, Restate
make build && make load                    # build all 5 service images and load into kind
make deploy                                # apply Helm release for stable Deployments + DestinationRules + default VirtualService rules
make e2e                                   # run the 13 acceptance scenarios

canary-ctl deploy-canary order-service v2  # create canary Deployment AND header rule
canary-ctl status order-service            # show current canary state
canary-ctl rollback order-service          # remove header rule then Deployment
canary-ctl reconcile order-service         # repair partial state
```

## Open questions and assumptions

- Restate.dev SDK ergonomics in TypeScript and Java are assumed sufficient to build the example handlers. Spike during plan execution if either SDK proves limiting.
- Strimzi Kafka operator is assumed appropriate for local kind; bitnami chart is the alternative if Strimzi is heavyweight on a developer laptop.
- Header propagation in lib-java relies on Spring's request-scoped beans plus interceptor wiring; async paths (e.g., `@Async`) require explicit context propagation. Library tests cover this surface.
- AsyncLocalStorage in lib-node propagates across `await` boundaries but not across worker threads; if any service uses worker threads in Phase 1, the library exposes an explicit `runInContext` helper.

## Phase 1 deliverable summary

A monorepo with five services (3 Java + Spring Boot, 2 TypeScript + Node) wired together over HTTP, Kafka (Strimzi), and Restate, deployed to a local `kind` cluster via Istio. Two shared platform libraries (`lib-java`, `lib-node`) implement `x-canary` header propagation across HTTP, Kafka, and Restate boundaries. A `canary-ctl` CLI is the single source of truth for the canary lifecycle, managing both the canary Deployment and the VirtualService header-match rule together with partial-state reconcile. Each service's resources are templated from one shared Helm chart with a canary values overlay. Thirteen canonical e2e scenarios verify routing correctness across canary topologies, isolation guarantees (Kafka and Restate stay stable-only in Phase 1), header propagation completeness, no-leak negative tests, and lifecycle safety. The full system runs and is exercised entirely on a developer laptop; remote CI/CD and cloud deploys are deferred to later phases.
