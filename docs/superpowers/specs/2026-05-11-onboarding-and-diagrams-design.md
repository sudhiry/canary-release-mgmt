# Onboarding doc + Mermaid diagrams — design

## Goal

Make `canary-release-mgmt` digestible to a new developer in a single
top-to-bottom read, with proper diagrams (not just ASCII) and a
worked-example dashboard walkthrough.

The existing docs (`architecture.md`, `canary-mechanics.md`,
`development.md`, `operations.md`, `history.md`) are accurate but
scattered. A new dev has to assemble the picture across five files,
none of which have sequence diagrams, and the dashboards get one line
each in `operations.md`. This spec closes those two gaps without
duplicating the existing prose.

The docs must reflect everything that has shipped through today:
**Phase 1 (HTTP canary) + Phase 2.a/2.b (Kafka canary) + Phase 3.a
(Restate substrate completion — real Restate-orchestrated saga) +
Phase 3.b (Restate canary handler versioning — β routing)**. Phases 4
and 5 are out of scope.

## Non-goals

- Replacing existing docs. ASCII art in `architecture.md` and
  `canary-mechanics.md` stays — Mermaid is additive.
- Documenting deferred phases (Phase 2.c schema evolution, Phase 4
  percent-split / Argo Rollouts, Phase 5 observability polish).
- Updating point-in-time phase specs under `docs/superpowers/specs/` —
  those are design artifacts, not living docs.
- A separate `dashboards.md` page. The dashboard walkthrough lives
  inside `onboarding.md` so a new dev sees it during the
  first-30-minutes flow.

## Files touched

| File | Action | What changes |
|---|---|---|
| `docs/onboarding.md` | **new** | Front-door file. See "Onboarding doc structure" below. |
| `docs/architecture.md` | **augment** | Add 2 Mermaid diagrams: system context, network topology. ASCII stays. |
| `docs/canary-mechanics.md` | **augment** | Add 4 Mermaid sequence diagrams: HTTP saga (Restate-orchestrated), Kafka K1, Restate β dispatch, presence-watch K5 takeover. |
| `docs/operations.md` | **augment** | Add 2 Mermaid sequence diagrams: canary-ctl deploy, canary-ctl rollback. Add one-line pointer to `onboarding.md`'s dashboard walkthrough. |
| `README.md` | **tiny tweak** | Add `docs/onboarding.md` as the first row in the "Documentation" table. |

## Onboarding doc structure

`docs/onboarding.md`, in order:

### 1. What this is (60 seconds)

One short paragraph: polyglot 5-service system (3 Java + 2 Node), HTTP
+ Kafka + Restate substrates, single `x-canary: true` header routes
canary on all three, runs on a laptop kind cluster behind Istio. Links
to the architecture deep-dive for the long version.

### 2. Mental model

One Mermaid **system context** diagram (the same one being added to
`architecture.md`) plus the four invariants stated plainly:

1. **Header propagates.** Every service that receives `x-canary: true`
   forwards it on every downstream HTTP / Kafka / Restate call —
   handled by `platform/lib-java` and `platform/lib-node`, never by
   service code.
2. **Per-subset Kafka groups.** Stable and canary consume the same
   topics via *different* consumer groups (`<svc>-stable` /
   `<svc>-canary`) so rebalances never move partitions between subsets.
3. **Presence-watch fallback.** Stable processes flagged messages only
   when canary is unhealthy. A long-lived k8s pod-watch flips the
   `canaryReady` flag in stable pods; the per-message Kafka filter
   reads it.
4. **Durable orchestration with variant-isolated handlers.** The
   `/api/orders` saga is orchestrated by Restate (Phase 3.a), not
   ad-hoc axios calls. Each variant registers under a distinct
   service name (`CheckoutSagaStable` / `CheckoutSagaCanary`); the
   order-service HTTP controller picks which one to invoke based on
   `x-canary` (Phase 3.b β routing).

### 3. Repo layout in 90 seconds

Re-use the tree block from `architecture.md`, each line annotated with
"go here when you want to X":

- `services/<svc>/` — change domain behavior
- `platform/lib-{java,node}/` — change header propagation or
  presence-watch
- `platform/restate-defs-{java,node}/` — change Restate service
  contracts (the *Stable / *Canary split lives here)
- `deploy/helm/` — change deployment shape (chart is shared across all
  5 services; per-service `values/` files; `canary-overlay.yaml` is
  the canary-only delta)
- `deploy/routing/` — Istio DestinationRule / VirtualService / Gateway
- `tools/canary-ctl/` — change lifecycle behavior
- `tools/traffic-cli/` — change the test client
- `tests/{infra,services,canary,e2e}/` — change test coverage

### 4. First 30 minutes (commands)

Verbatim copy-paste sequence with a "what just happened" callout after
each block. The shape:

```bash
# 1. Clone + workspace deps (~30s)
git clone <repo> && cd canary-release-mgmt
pnpm install

# 2. Verify your toolchain (~2 min, no cluster)
make verify
# → Runs all Java + Node unit tests. If this passes, your toolchain is set up.

# 3. Bring up the substrate (~4 min)
make up
make smoke-infra
# → kind cluster + Istio + Strimzi/Kafka + Restate + observability. 11 smoke assertions.

# 4. Build + deploy services (~3 min)
make build-services && make build-images && make load-images
make deploy-services
make smoke-services
# → All 5 services running stable, registered with Restate, joined Kafka groups.

# 5. Send your first order (baseline)
node tools/traffic-cli/bin/traffic-cli order
# → 201, status=completed, x-served-version: stable on every hop.

# 6. Deploy a canary
make canary-deploy SVC=payment-service TAG=dev
make canary-status SVC=payment-service
# → Helm release payment-service-canary, VirtualService has canary-by-header rule.

# 7. Send a flagged order
node tools/traffic-cli/bin/traffic-cli order --canary
# → Response includes x-served-chain showing payment-service=canary, others stable.

# 8. (Now open the dashboards — section 5 below)

# 9. Tear down the canary
make canary-rollback SVC=payment-service

# 10. Tear down the cluster (when done)
make down
```

### 5. Manual dashboard walkthrough

Worked example: a canary is deployed on `payment-service` (step 6
above) and flagged traffic is flowing (step 7). Walk through each
dashboard with what to look for.

**5.1 Open the dashboards**

```bash
make dashboards         # background port-forwards
make dashboards-status  # verify all four are up
```

| Dashboard | URL |
|---|---|
| Kiali | http://localhost:20001 |
| Grafana | http://localhost:3000 |
| Prometheus | http://localhost:9090 |
| Jaeger | http://localhost:16686 |

**5.2 Kiali — see the traffic split visually**

1. Navigate: Graph → Namespace `services` → Display: "Versioned app
   graph".
2. Expected picture: `order-service` → `payment-service` shows two
   edges, one to the `stable` subset, one to the `canary` subset.
3. Send 5 baseline + 5 flagged orders (`traffic-cli order` x5 /
   `traffic-cli order --canary` x5) and watch the edge weights update.
4. Smoke check: zero traffic on the canary edge from baseline
   requests; 100% of canary edge traffic carries `request_protocol=http`
   with the `x-canary` header.

**5.3 Jaeger — trace a flagged request end-to-end**

1. Service: `order-service.services`.
2. Tags filter: `x-canary=true`.
3. Pick the latest trace; inspect the span tree:
   - `order-service` (entry span)
   - → `inventory-service`, `payment-service`, `notification-service`
     (sibling axios spans)
   - Each span carries `x-served-version=stable|canary` as a process
     tag — the multi-hop routing decision is visible without reading
     logs.

**5.4 Grafana — per-version metrics**

1. Dashboards → "Istio Workload Dashboard".
2. Workload filter: `payment-service-stable`, then
   `payment-service-canary` — request rate / p99 latency / error rate
   side by side.
3. The S7 e2e scenario asserts stable's p99 stays within 1.5× baseline
   during canary deploy; this is where you'd see a regression visually.

**5.5 Prometheus — one canned query**

```promql
sum(rate(istio_requests_total{destination_workload=~"payment-service.*"}[1m]))
  by (destination_workload)
```

Two series, one per subset. Useful as a CI alert: anomaly if canary
RPS > stable RPS for an unflagged-traffic workload.

**5.6 Restate admin — inspect handler registration (Phase 3.a / 3.b)**

Restate doesn't ship a dashboard in this repo. Use the admin HTTP
API directly:

```bash
# All registered deployments (one per service pod)
curl -s http://localhost:9070/deployments | jq '.deployments[] | {id, services: [.services[].name]}'

# Expected services per stable+canary pair:
#   stable pod:  CheckoutSagaStable, ReservationWorkflowStable, PaymentVOStable, NotificationServiceStable
#   canary pod:  CheckoutSagaCanary, ReservationWorkflowCanary, PaymentVOCanary, NotificationServiceCanary

# List active invocations (requires the `restate` CLI, installed with the server).
# Exact filter syntax depends on Restate CLI version; the implementation plan
# will pin the form used in this repo.
restate --address http://localhost:9070 invocations list

# If you only have curl: the /deployments query above is usually enough to
# confirm β routing is wired correctly — distinct service names per pod is the
# load-bearing invariant.
```

This is the only way to see Phase 3.b's β routing working: stable and
canary register under *distinct* service names, and the order-service
controller picks the right one per request based on the header.

**5.7 Cleanup**

```bash
make canary-rollback SVC=payment-service
# Kiali's canary edge disappears within a few seconds.
```

### 6. Where to go next

| If you want to... | Read |
|---|---|
| Understand the system map and per-service stack | `docs/architecture.md` |
| Understand `x-canary` propagation in depth | `docs/canary-mechanics.md` |
| Set up your local toolchain / Spring Boot 4 quirks | `docs/development.md` |
| Deploy, troubleshoot, run e2e scenarios | `docs/operations.md` |
| See what shipped in each phase and why | `docs/history.md` |
| See the design + plan for a specific phase | `docs/superpowers/{specs,plans}/` |

## Mermaid diagrams — full inventory

### A. System context (added to `architecture.md`)

Boxes for: the test client, Istio Gateway, the 5 services grouped by
stack, Kafka cluster (one box, 5 topics annotated), Restate
cluster. Edges labeled with substrate (HTTP / Kafka / Restate).
Purpose: replace the one-shot understanding currently encoded in the
ASCII art with a clickable / zoomable diagram.

### B. Network topology (added to `architecture.md`)

K8s namespaces as subgraphs (`istio-system`, `kafka`, `restate`,
`services`). Inside `services`, show DestinationRule subsets
(`stable` + `canary`) as separate boxes per service. Gateway port
mapping (`localhost:8080` → `istio-ingressgateway`) and Restate
admin port mapping (`localhost:9070` → `restate.restate:9070`)
shown as edges crossing the cluster boundary.

### C. HTTP saga sequence — Restate-orchestrated (added to `canary-mechanics.md`)

Participants: client, Istio Gateway, order-service HTTP controller,
Restate Ingress, CheckoutSaga (Restate handler in order-service pod),
inventory-service, payment-service, notification-service.

Flow:

1. `POST /api/orders` with `x-canary: true`.
2. Istio Gateway → order-service VirtualService → canary-by-header
   rule → order-service-canary pod.
3. Order-service HTTP controller reads header → picks
   `CheckoutSagaCanary` (β routing, Phase 3.b).
4. Controller POSTs `/CheckoutSagaCanary/<orderId>/run` to Restate
   Ingress.
5. Restate Ingress dispatches to the registered deployment URL
   (variant-isolated K8s Service `order-service-canary`) — the
   CheckoutSaga handler runs in the canary pod.
6. Handler invokes (HTTP, x-canary stamped on each call):
   - `inventory-service` ReservationWorkflow
   - `payment-service` PaymentVO
   - `notification-service`
7. Each downstream service's VirtualService routes
   by-header → canary subset (if deployed) else stable.
8. Response includes `x-served-chain: order-service=canary,
   payment-service=canary, ...`.

### D. Kafka per-subset routing K1 (added to `canary-mechanics.md`)

Participants: order-service-canary, Kafka topic `orders.events`,
audit-service-canary (consumer group `audit-service-canary`),
audit-service-stable (consumer group `audit-service-stable`).

Flow:

1. order-service-canary produces a record with header `x-canary=true`
   (stamped by `XCanaryKafkaProducerInterceptor`).
2. Both consumer groups receive the record (different group IDs, same
   topic).
3. audit-service-canary's `XCanaryConsumeFilter.shouldProcess()` →
   `true` (own=canary, header=true). Records to internal store.
4. audit-service-stable's filter → `false` (own=stable, header=true,
   canaryReady=true → skip).
5. K1 assertion: only canary's `/internal/consumed-events` shows the
   record.

### E. Restate β handler dispatch (added to `canary-mechanics.md`)

Participants: order-service HTTP controller, Restate Ingress, Restate
deployment registry, payment-service-stable pod, payment-service-canary
pod.

Flow (the "how does Restate know where to send each variant" diagram):

1. On startup, each pod registers its handlers with Restate Admin via
   the per-pod K8s Service URL: `http://<svc>-stable.services:9081/`
   or `http://<svc>-canary.services:9081/`.
2. Stable pod registers `PaymentVOStable`; canary pod registers
   `PaymentVOCanary`. **Distinct service names** are the load-bearing
   bit.
3. Saga handler picks the variant by `x-canary` header at
   invocation time.
4. Restate Ingress looks up the service-name → deployment-URL
   mapping; routes to the variant-specific K8s Service.
5. Variant isolation enforced by 3 independent layers (registration
   under distinct names, in-saga client construction, K8s endpoint
   selection — Restate's pods sit outside the Istio mesh so they
   can't use subset routing).

### F. Presence-watch K5 takeover (added to `canary-mechanics.md`)

Participants: audit-service-canary (consumer), Kubelet,
EndpointSlice controller, audit-service-stable's
`XCanaryPresenceWatcher`, audit-service-stable's Kafka listener,
Kafka topic `orders.events`.

Flow:

1. Canary's Kafka consumer wedges (e.g. SIGSTOP, GC pause, broker
   blip).
2. `last-heartbeat-seconds-ago` (Java) /
   `consumer.events.HEARTBEAT` (Node) goes stale beyond
   `KAFKA_HEARTBEAT_STALE_MS` (15s default).
3. `KafkaConsumerHealthIndicator` reports `OUT_OF_SERVICE`.
4. Canary's `/health/readiness` (Spring Actuator `kafkaConsumer`
   group, canary-only) returns 503.
5. Kubelet flips pod `Ready=False`; EndpointSlice controller drops
   the canary endpoint.
6. Stable's `XCanaryPresenceWatcher` (long-lived
   `Pod.watch(labelSelector=app=<svc>,version=canary)`) receives a
   MODIFIED event with `Ready=False`. Flips `canaryReady = false`
   atomically.
7. Next flagged record arrives at stable's Kafka listener →
   `XCanaryConsumeFilter.shouldProcess()` → `true` (header=true,
   own=stable, canaryReady=false → graceful fallback).
8. Stable processes the record.

### G. canary-ctl deploy (added to `operations.md`)

Participants: developer (`make canary-deploy`), canary-ctl, state
file (`~/.canary-ctl/<svc>.json`), Helm, K8s API, Istio
VirtualService.

Flow:

1. `make canary-deploy SVC=<s> TAG=<t>`.
2. canary-ctl writes state `phase: deploying`.
3. `helm upgrade --install <svc>-canary --wait` → rollout runs.
   - Branch on failure: auto-rollback (uninstall, ensure VS rule is
     default-only, clear state), re-throw.
4. State `phase: deployment-ready`.
5. Patch VirtualService — insert `canary-by-header` rule above
   `default`.
6. State `phase: active`.

### H. canary-ctl rollback (added to `operations.md`)

Participants: developer, canary-ctl, state file, Istio
VirtualService, Helm.

Flow:

1. `make canary-rollback SVC=<s>`.
2. State `phase: rolling-back`.
3. Patch VirtualService back to `default`-only (no new flagged
   traffic reaches canary).
4. Sleep `--grace-seconds` (default 10s) — in-flight requests drain.
5. `helm uninstall <svc>-canary`.
6. Delete state file.

Each step is idempotent; running rollback on a clean cluster is a
no-op.

## Acceptance criteria

A new developer reading `docs/onboarding.md` top-to-bottom should be
able to:

1. Explain the four invariants (header propagation, per-subset Kafka
   groups, presence-watch fallback, durable variant-isolated
   handlers) in their own words.
2. Run the first-30-minutes commands cold and end up with a working
   canary-deployed cluster.
3. Open every dashboard listed and identify the canary in it —
   visually in Kiali, in a Jaeger trace, in Grafana metrics, in
   Prometheus, and via the Restate admin API.
4. Know which deep-dive doc to open for any of: per-component code,
   environment setup, e2e troubleshooting, phase history.

All 8 Mermaid diagrams must render in GitHub's web preview (validated
locally with the GitHub markdown preview extension or `gh pr view
--web` on a draft PR). ASCII art in the existing docs must remain
intact alongside the Mermaid additions.

## Risks / open questions

- **Mermaid sequence diagrams with many participants get cramped.**
  Mitigation: keep each diagram to ≤7 participants; the K5 takeover
  diagram is the worst case and is already at 7. If readability
  suffers, split E or F into two diagrams.
- **Dashboard URLs / dashboard names drift if we upgrade Istio
  addons.** Mitigation: the walkthrough sources URLs from the
  existing `operations.md` table — any addon version change updates
  one place.
- **`docs/onboarding.md` could rot.** Mitigation: it deliberately
  duplicates *nothing* from the deep-dive docs except the system
  context diagram (which lives canonically in `architecture.md`).
  The first-30-minutes commands are also in `README.md`'s TL;DR;
  these need to stay in sync. Acceptable because the README block is
  short and changes rarely (it's tied to `make` target names).
