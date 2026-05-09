# Canary Release Management — Phase 1.5.b Design (12 remaining e2e scenarios S2–S13)

**Status:** Approved (awaiting user review)
**Date:** 2026-05-09
**Phase:** Phase 1, sub-plan 5.b of 5 (final Phase 1 sub-plan)
**Umbrella spec:** `docs/superpowers/specs/2026-05-08-canary-release-phase-1-design.md`
**Predecessor:** `docs/superpowers/specs/2026-05-09-canary-release-phase-1-5-a-e2e-foundation-design.md`

## Project context

Plan 1.5.a shipped the e2e harness foundation (vitest, helpers for canary lifecycle, traffic, subset assertion, Kafka admin, Restate admin, load gen) plus the first scenario (S1 Baseline). Plan 1.5.b implements the remaining twelve scenarios from the umbrella spec's canonical acceptance table (S2–S13) and adds three small infrastructure pieces those scenarios need:

1. **Per-hop subset chain** — extends the `x-served-version` response-header pattern from 1.5.a so multi-hop scenarios (S3, S4, S8) can verify the version of *every* service in the chain, not just the immediate handler.
2. **Pod log scraping helper** — needed by S9 to assert canary pods log no inbound requests when the request carried no `x-canary` header.
3. **Kafka host port-forward helper** — needed by S10 to query Kafka admin (consumer-group membership) from the test process; the in-cluster broker isn't reachable from the host without a port-forward.

Plan 1.5.b is the last Phase 1 sub-plan. After it merges, Phase 1 is complete and the project is ready for Phase 2 (Kafka canary consumer strategies).

## Goals

1. Implement scenarios S2–S13 as one vitest test file each in `tests/e2e/`, all driven by `make e2e` (full set) and the curated subset by `make ci-local` (S1, S2, S5, S8, S9, S12 per the umbrella spec).
2. Add per-hop chain instrumentation to `lib-java` and `lib-node` so every service contributes its identity + version to a response header (`x-served-chain`) that the test can parse to verify multi-hop routing.
3. Add the pod-log and Kafka port-forward helpers needed by S9 and S10.
4. Pass all 13 scenarios end-to-end against a fresh `make up && make build-services && make build-images && make load-images && make deploy-services` cluster within ~15 minutes.

## Non-goals (Plan 1.5.b)

- Anything beyond Phase 1 — Kafka canary consumer routing (Phase 2), Restate canary handler versioning (Phase 3), Argo Rollouts / percent-split / GitHub Actions CI (Phase 4), production observability polish (Phase 5).
- Distributed tracing integration (Jaeger query API). Per-hop chain via response header is the chosen mechanism; Jaeger remains as an operator-visible tool but not a test integration.
- A statistical-significance latency comparison for S7. Plan 1.5.b uses a fixed-tolerance baseline-vs-canary delta.
- Pre-flight cluster bootstrap from inside the test (`make up` and image build/load are still operator concerns).

## Locked decisions

The following are settled before this design and not revisited:

- **Multi-hop verification mechanism: per-hop response header chain (`x-served-chain`).** Each service prepends its `<svc>=<version>` token onto a comma-separated chain header it returns; downstream call interceptors capture the downstream's chain so the upstream can prepend onto it. No external dependencies (Jaeger client, trace ID extraction). Same pattern as the existing `x-canary` propagation in Plan 1.2 + `x-served-version` in Plan 1.5.a.
- **Pod log scraping: `kubectl logs --since=<seconds>` shell-out.** Helper wraps the existing kubectl shell-out machinery; no Kubernetes API client, no streaming.
- **Kafka host access: per-test `kubectl port-forward` to `svc/my-cluster-kafka-bootstrap`.** Helper spawns a child process in `beforeAll`, kills it in `afterAll`. No permanent change to the Kafka CR (which currently has only an internal listener).
- **S6 trigger: bad image tag.** Same mechanism Plan 1.4's bats smoke uses to force auto-rollback.
- **S7 latency: fixed-tolerance delta.** Capture stable load p99 baseline; deploy canary; capture stable load p99 during canary deploy; assert the during-canary p99 is within 1.5× of baseline. Tolerance is a constant in the test; it can be tuned once the kind cluster's natural noise is observed.
- **`make ci-local` subset: S1, S2, S5, S8, S9, S12** (per umbrella spec §"Continuous local verification").

## Architecture

### Per-hop subset chain — header format

`x-served-chain` is a comma-separated list of `<service>=<version>` tokens, ordered request-flow direction (outermost service first):

```
x-served-chain: order-service=canary,inventory-service=canary,audit-service=stable,payment-service=stable,audit-service=stable,notification-service=stable,audit-service=stable
```

Each service appears once per call path. `audit-service` appears multiple times because the spec's call graph has it called from inventory, payment, and notification independently — that's correct and useful (each occurrence's version is asserted independently).

For scenarios that don't care about the full chain (S2, S5, etc.), tests use the existing `x-served-version` header from 1.5.a (which only reports the immediate handler).

### Per-hop subset chain — implementation

**Java side (`platform/lib-java/`):**

Two new classes:

- `XServedChainContext` — `ThreadLocal<List<String>>` accumulator. Cleared per request. The HTTP response filter reads this when building the response header.
- `XServedChainResponseFilter` — servlet filter that runs late in the chain. Before writing the response, it builds `x-served-chain` from `<own service>=<own version>` + accumulator contents.
- `XServedChainRestClientInterceptor` — outbound HTTP interceptor. After the downstream call returns, reads the response's `x-served-chain` header and appends each token to the context accumulator.

`XCanaryAutoConfiguration` registers the new filter and interceptor. The interceptor is added to the existing `RestClient.Builder` customizer alongside `XCanaryRestClientInterceptor`.

The own-service name comes from a Spring property `canary.service-name` (or the `SERVICE_NAME` env var; defaults to "unknown"). The own-version comes from the existing `canary.version` / `VERSION` env var from Plan 1.5.a.

**Node side (`platform/lib-node/`):**

Mirror set:

- `xServedChainContext` — AsyncLocalStorage-backed accumulator.
- `xServedChainMiddleware()` — Express middleware. Reads `SERVICE_NAME` + `VERSION` env vars at factory time. On response (using `res.on("finish")` or by patching `res.send`), builds the chain header from `<own>=<version>` + context accumulator.
- `attachXServedChainAxiosInterceptor()` — axios response interceptor. Reads `x-served-chain` from each response and appends tokens to the accumulator.

Each Node service adds two lines to its app setup: `app.use(xServedChainMiddleware())` and `attachXServedChainAxiosInterceptor(axiosInstance)`.

**Helm chart change:**

Add `SERVICE_NAME` env var (sourced from `.Values.serviceName`, which is already used in the chart for resource naming). One line addition to `configmap.yaml` alongside the existing `VERSION` key.

### Pod log scraping helper

`tests/e2e/helpers/pod-logs.ts`:

```typescript
export interface PodLogQueryOpts {
  namespace: string;
  labelSelector: string;       // e.g. "app=order-service,version=canary"
  sinceSeconds?: number;       // default 60
}

export async function getPodLogs(opts: PodLogQueryOpts): Promise<string>;
export function logsContain(logs: string, pattern: RegExp | string): boolean;
```

Implementation: `kubectl logs -n <namespace> -l <labelSelector> --since=<n>s --tail=-1 --prefix=true`. Returns combined stdout from all matching pods. The `--prefix=true` flag adds `[pod/<name>]` to each line so callers can disambiguate. `logsContain` is a thin wrapper for assertion clarity.

S9 will use this to assert: after sending a no-header request, no canary pod's logs contain the request's user ID (which is unique per scenario run).

### Kafka host port-forward helper

`tests/e2e/helpers/kafka-port-forward.ts`:

```typescript
export interface KafkaPortForward {
  stop: () => Promise<void>;
}

export async function startKafkaPortForward(localPort?: number): Promise<KafkaPortForward>;
```

Spawns `kubectl port-forward -n kafka svc/my-cluster-kafka-bootstrap <localPort>:9092` as a detached child. Polls `localhost:<localPort>` with a TCP connect until ready (timeout 30s). Returns a handle whose `stop()` SIGTERMs the child and awaits exit. Default `localPort` is 9092 (matches the kafka-admin helper's default broker).

S10 uses this in `beforeAll` to start the forward; `afterAll` stops it.

### S6 unhealthy-canary trigger (no new chart variant needed)

S6 reuses the standard `canary-overlay.yaml`. The trigger is a bad image tag passed at deploy time: `canary-ctl deploy-canary payment-service does-not-exist-bogus-tag`. The bad tag forces ImagePullBackOff → rollout deadline → canary-ctl's auto-rollback fires. This is the same mechanism Plan 1.4's bats smoke test already exercises, so no new YAML or chart variant is needed.

### Scenarios — ordered by helper dependency

Each scenario is one file under `tests/e2e/`. All scenarios share a common `beforeAll` pattern that ensures a clean baseline (no canary on any service) before running.

**Tier 1 — uses only existing helpers from 1.5.a:**

| # | File | What it does | Asserts |
|---|---|---|---|
| S5 | `s5-no-canary-fallback.test.ts` | All stable cluster. Header request. | Same as S1's second `it`. Spec dedicates a separate file for clarity. |
| S6 | `s6-canary-unhealthy.test.ts` | Deploy canary with bad tag. | `canary-ctl deploy-canary` exits non-zero; auto-rollback fires; final status is clean. Uses canary helper only. |
| S11 | `s11-restate-isolation.test.ts` | Deploy canary on payment. Query Restate Admin for deployments. | Restate registry contains stable services only; canary did NOT register. Uses restate-admin helper. |
| S12 | `s12-rollback.test.ts` | Deploy canary on payment. Verify active. Rollback. Verify clean. | After rollback: header rule absent; canary release absent; state file absent; subsequent header request → stable. Uses canary + traffic + subset helpers. |
| S13 | `s13-partial-state-recovery.test.ts` | Deploy canary on payment. Manually delete the VS header rule (simulating partial state). Run `canary-ctl reconcile`. | Reconcile reports `drift-fix`; subsequent state is consistent. Uses canary helper + direct kubectl shell-out (already available via existing helpers). |

**Tier 2 — adds pod-log helper (S9) and kafka port-forward + admin (S10):**

| # | File | What it does | Asserts | New infra |
|---|---|---|---|---|
| S9 | `s9-header-leak-prevention.test.ts` | Deploy canary on all 5. Send a request WITHOUT `x-canary` header containing a unique user ID. | No canary pod's logs contain the unique user ID. | pod-logs |
| S10 | `s10-kafka-isolation.test.ts` | Deploy canary on order-service (a Kafka producer). Query Kafka admin for consumer-group members of `order-service-events-group` (or whatever group name). | Only stable-pod IDs in the member list; no canary pod IDs. | kafka-port-forward + existing kafka-admin |

**Tier 3 — adds per-hop chain mechanism (S3, S4, S8 + S2 if we use chain):**

| # | File | What it does | Asserts | New infra |
|---|---|---|---|---|
| S2 | `s2-single-svc-canary.test.ts` | Deploy canary on payment-service. Send header request through edge. | Chain contains `payment-service=canary`, others `=stable`. | per-hop chain |
| S3 | `s3-multi-svc-canary.test.ts` | Deploy canary on order-service AND inventory-service. Send header request. | Chain contains `order-service=canary, inventory-service=canary, payment-service=stable, notification-service=stable, audit-service=stable`. | per-hop chain |
| S4 | `s4-full-chain-canary.test.ts` | Deploy canary on all 5. Send header request. | Every entry in the chain has `=canary`. | per-hop chain |
| S8 | `s8-header-propagation.test.ts` | Deploy canary on all 5. Send header request. | Chain has 7 entries (one per call-graph node, including audit's 3 occurrences). All `=canary`. Verifies header propagation hit every internal hop. | per-hop chain |

**Tier 4 — adds load gen baseline (S7):**

| # | File | What it does | Asserts | New infra |
|---|---|---|---|---|
| S7 | `s7-stable-undisrupted.test.ts` | Run 50 rps × 30s stable load (no canary deployed). Capture p99. Deploy canary on payment-service. Run another 50 rps × 30s stable load. Capture p99. Roll back. | Second p99 is ≤ 1.5× first p99. Zero stable failures during canary deploy. | existing load helper |

### Test execution + isolation

- vitest's sequential single-fork pool from 1.5.a is unchanged.
- Each scenario's `beforeAll` calls a shared `ensureCleanBaseline()` helper (new, in `tests/e2e/helpers/cluster.ts`) that calls `canary-ctl rollback <svc>` for all 5 services (idempotent — no-ops if already clean).
- Each scenario's `afterAll` rolls back any canaries IT deployed.
- Total runtime estimate: ~10–15 min for the full 13 scenarios. The longest is S7 (~90s for two 30s loads). The shortest is S5 (~5s — no cluster mutation).

### Make targets

`make e2e` is unchanged (runs all scenarios with `E2E_SCENARIOS=1`).

`make ci-local` updates from "just S1" to the spec's curated subset:

```makefile
ci-local: ## Run fast e2e subset (S1, S2, S5, S8, S9, S12 per umbrella spec)
	@pnpm --filter @canary/e2e build >/dev/null
	@E2E_SCENARIOS=1 pnpm --filter @canary/e2e exec vitest run "s(1|2|5|8|9|12)-"
```

The vitest CLI accepts a regex against test file names, so `s(1|2|5|8|9|12)-` matches `s1-baseline.test.ts`, `s2-single-svc-canary.test.ts`, etc.

## Data flow

### S2 (single-service canary, with chain verification)

1. `beforeAll`: `ensureCleanBaseline()` rolls back all 5 services (idempotent).
2. Scenario step 1: `deployCanary("payment-service", "dev")` — installs `payment-service-canary` Helm release; applies VS header rule.
3. Scenario step 2: `sendOrder({ canary: true })` → POST to ingress with `x-canary: true`.
4. Request flow:
   - Ingress → order-service VS evaluates header rule (no canary on order; rule absent) → stable order-service.
   - order-service propagates `x-canary: true` to downstreams (per Plan 1.2 lib).
   - inventory-service VS: no canary on inventory; stable serves.
   - payment-service VS: canary on payment AND header set → header rule routes to canary subset → canary payment serves.
   - notification-service VS: no canary; stable serves.
   - Each service stamps `x-served-chain` on its response. order-service captures inventory's, payment's, notification's chains via the axios interceptor; combines them; stamps own.
5. Response arrives at test with `x-served-chain: order-service=stable,inventory-service=stable,...,payment-service=canary,...,notification-service=stable,...`.
6. Test parses the chain, asserts: `payment-service=canary` IS present; `order-service=canary` IS NOT present; etc.
7. `afterAll`: `rollback("payment-service")` — idempotent.

### S9 (header leak prevention, with pod-log helper)

1. `beforeAll`: clean baseline. Then `deployCanary("payment-service", "dev")` — canary deployed.
2. Scenario sends one request **without** `x-canary` header carrying user ID `s9-leak-test-${randomUUID()}` (unique per test run).
3. Stable-only routing serves; canary payment pod sees nothing.
4. Test waits ~3s for any logs to land.
5. Test calls `getPodLogs({ namespace: "services", labelSelector: "app=payment-service,version=canary", sinceSeconds: 30 })`.
6. Asserts the unique user ID does NOT appear in canary pod logs.
7. `afterAll`: rollback.

### S10 (Kafka isolation, with port-forward + kafka-admin)

1. `beforeAll`: clean baseline. `startKafkaPortForward()`. Then `deployCanary("order-service", "dev")` — canary order producing Kafka events.
2. Scenario sends a header request → flows through canary order → produces an event with `x-canary: true` Kafka header.
3. Test calls `kafka.connect()`, then `kafka.consumerGroupMembers("orders-events-consumers")` (the Kafka consumer group joined by stable order-service consumers).
4. Asserts: every member's host/clientId resolves to a stable-version pod (not a canary pod). Canary order-service has `KAFKA_CONSUMERS_ENABLED=false` per the canary-overlay so it should not have joined.
5. `afterAll`: `kafka.disconnect()`, `stop()` the port-forward, rollback canary.

The exact consumer-group name depends on the service implementation. The test will discover groups via `kafka-admin.listConsumerGroups()` and pick the one that matches the order-service stable subset's pattern. If group naming proves inconsistent during implementation, the scenario will use a more permissive assertion (e.g., "no member ID contains '-canary-'").

## Error handling

Same model as 1.5.a: each helper throws on failure with a clear message; vitest reports it as a test failure. Specific failure modes:

- **Cluster pre-condition fails** (canary already deployed before test starts): `ensureCleanBaseline()` would have rolled it back. If rollback itself fails, the test fails with the canary-ctl error verbatim.
- **Per-hop chain header missing or malformed**: helper throws `expected x-served-chain header but got: [...]` or `chain has N tokens, expected M`. Indicates a service hasn't been re-deployed with the new lib changes.
- **Pod logs unreachable** (kubectl error): helper throws with the kubectl stderr. Likely cause: cluster down, or namespace wrong.
- **Kafka port-forward fails to start within 30s**: helper throws `port-forward failed to become ready: <stderr>`. Likely cause: kubectl port-forward already running on the same port from a prior aborted test.
- **Test interrupted mid-scenario**: leaves whatever canary state was being tested. Next test run's `beforeAll` cleans up via `canary-ctl rollback`.

## Testing strategy

### What 1.5.b tests itself

1. **Lib unit tests** for the chain mechanism:
   - Java: `XServedChainContextTest`, `XServedChainResponseFilterTest`, `XServedChainRestClientInterceptorTest` (~6–8 tests total). Cover empty chain, single-hop, multi-hop accumulation, response-header serialization, missing-env defaults.
   - Node: equivalent vitest tests on `xServedChainContext`, `xServedChainMiddleware`, `attachXServedChainAxiosInterceptor` (~6–8 tests).
2. **No unit tests for new e2e helpers** (`pod-logs.ts`, `kafka-port-forward.ts`, `chain.ts`) — same rationale as 1.5.a. They are thin wrappers around well-tested deps; verified by the scenarios that use them.
3. **The 12 scenarios themselves** — these ARE the integration tests. Each runs against a real cluster.

### What requires a real cluster

All 13 scenarios. Same model as 1.5.a (operator runs `make e2e` after refreshing images).

### Continuous local verification

`make verify` (existing) — Java + Node unit tests, runtime ~30s.

`make ci-local` (updated) — fast e2e subset (S1, S2, S5, S8, S9, S12), runtime ~5–6 minutes.

`make e2e` (existing) — all 13, runtime ~10–15 minutes.

## Repo additions

```
platform/lib-java/src/main/java/com/canary/platform/lib/
├── XServedChainContext.java                          # NEW — ThreadLocal accumulator
├── XServedChainResponseFilter.java                   # NEW — emits x-served-chain
└── XServedChainRestClientInterceptor.java            # NEW — captures downstream chain
+ tests for all 3

platform/lib-java/src/main/java/com/canary/platform/lib/autoconfigure/
└── XCanaryAutoConfiguration.java                     # MODIFY — register new beans + interceptor

platform/lib-node/src/
├── x-served-chain-context.ts                         # NEW — AsyncLocalStorage accumulator
├── x-served-chain-middleware.ts                      # NEW — Express middleware
├── x-served-chain-axios.ts                           # NEW — axios interceptor
└── index.ts                                          # MODIFY — re-exports
+ tests in src/__tests__/ for all 3 new modules

services/order-service/src/                           # MODIFY — wire chain middleware + axios interceptor
services/notification-service/src/                    # MODIFY — same

deploy/helm/service-chart/templates/configmap.yaml    # MODIFY — add SERVICE_NAME key

tests/e2e/helpers/
├── chain.ts                                          # NEW — parse + assert on x-served-chain
├── pod-logs.ts                                       # NEW — kubectl logs --since wrapper
├── kafka-port-forward.ts                             # NEW — kubectl port-forward wrapper
└── cluster.ts                                        # NEW — ensureCleanBaseline()

tests/e2e/                                            # NEW scenario files
├── s2-single-svc-canary.test.ts
├── s3-multi-svc-canary.test.ts
├── s4-full-chain-canary.test.ts
├── s5-no-canary-fallback.test.ts
├── s6-canary-unhealthy.test.ts
├── s7-stable-undisrupted.test.ts
├── s8-header-propagation.test.ts
├── s9-header-leak-prevention.test.ts
├── s10-kafka-isolation.test.ts
├── s11-restate-isolation.test.ts
├── s12-rollback.test.ts
└── s13-partial-state-recovery.test.ts

Makefile                                              # MODIFY — ci-local subset
README.md                                             # MODIFY — Plan 1.5.b section
```

## Operator workflow (after Plan 1.5.b)

```
make up                                                 # 1.1
make build-services                                     # 1.3.a + lib changes from 1.5.a + 1.5.b
make build-images && make load-images                   # 1.3.b
make deploy-services                                    # 1.3.b
make smoke-services                                     # 1.3.b
make smoke-canary                                       # 1.4

# 1.5 additions:
make ci-local                                           # fast subset (~5 min)
make e2e                                                # full 13 scenarios (~15 min)
make e2e SCENARIO=s7                                    # single scenario
```

## Done when

- All unit tests pass: `make verify` runs cleanly with the new lib chain tests included.
- `pnpm --filter @canary/e2e build` produces clean dist artifacts.
- `make e2e` passes all 13 scenarios against a fresh `make up && make deploy-services` cluster (with refreshed images).
- `make ci-local` runs only the curated subset and passes.
- README has a `## Plan 1.5.b` section.
- All commits in the task list are present on `claude/phase-1.5.b-scenarios`.

## Open assumptions

- The 5 services' Kafka consumer-group naming is consistent enough for S10 to assert on (verified during scenario implementation; fallback to "no -canary- in member ID" if not).
- `kubectl port-forward` from the test host can reach `svc/my-cluster-kafka-bootstrap` in the `kafka` namespace (it can — this is the same path Strimzi documents).
- The kind cluster's natural latency variance is small enough that S7's 1.5× p99 tolerance won't flake. If it does, the tolerance is a constant we can tune.
- Each service's lib upgrade (chain mechanism) is backward-compatible: a request that doesn't include `x-served-chain` is fine; a downstream response that doesn't include it is fine. The chain is purely additive.
- Audit-service appearing 3× in the chain (one per upstream caller) is the intended behavior, not duplicate-detection material.
