# Canary Release Management — Phase 1.5.a Design (e2e harness foundation + S1 Baseline)

**Status:** Approved (awaiting user review)
**Date:** 2026-05-09
**Phase:** Phase 1, sub-plan 5.a of 5
**Umbrella spec:** `docs/superpowers/specs/2026-05-08-canary-release-phase-1-design.md`

## Project context

Phase 1 of the canary release reference architecture has shipped its substrate (Plans 1.1, 1.2, 1.3.a, 1.3.b) and the per-service canary lifecycle tool (Plan 1.4 — `canary-ctl` + `traffic-cli`). What remains is the verification surface: thirteen canonical end-to-end scenarios that prove the canary mechanism works correctly across positive routing, negative isolation, and lifecycle dimensions.

Plan 1.5 was decomposed into two sub-plans during brainstorming:

- **Plan 1.5.a (this spec)** — TS e2e harness foundation, helpers, subset-served instrumentation, load generator, and the first scenario (**S1 Baseline**) end-to-end. Proves the harness works against a real cluster.
- **Plan 1.5.b** — the remaining twelve scenarios (S2–S13).

This decomposition mirrors the 1.3.a/1.3.b split: foundation first, then the bulk of the work that consumes it. Each sub-plan ships independently mergeable, working software.

## Goals

1. Stand up a TypeScript e2e harness in `tests/e2e/` using **vitest**, configured for cluster-mutation tests (sequential, long timeouts, one file per scenario).
2. Add minimal **subset-served instrumentation** to `lib-java` and `lib-node` so tests can trivially assert which version (stable vs canary) handled a request, plus the corresponding Helm chart change to pass the version into containers as an env var.
3. Provide reusable **helpers** for the most common e2e operations: invoking `canary-ctl`, sending traffic, asserting subset-served, querying Kafka admin, querying Restate admin, and running a small TS-native load generator.
4. Implement **S1 Baseline** end-to-end as proof the foundation works.
5. Wire `make e2e`, `make e2e SCENARIO=<name>`, `make ci-local` Make targets.

## Non-goals (Plan 1.5.a)

- Scenarios S2–S13 — Plan 1.5.b.
- Multi-hop chain verification (a request flowing through `order → payment → audit` and asserting *each hop's* version). The single-hop response header from the immediate handler is sufficient for S1 and the simpler scenarios; multi-hop verification design is deferred to 1.5.b along with whichever scenarios need it (likely S3, S4, S8).
- Pod log scraping (S9 needs this — deferred to 1.5.b).
- Latency-distribution measurement against a stable load (S7 needs this — deferred to 1.5.b).
- Integration with Jaeger trace queries (none of the 1.5.a scenarios need it).
- Conversion of canary-ctl invocation to direct TS module imports (the helpers shell out to the binary — see "Locked decisions").
- Canary serving for Kafka or Restate (Phase 2 + Phase 3 respectively, per umbrella spec). The Kafka and Restate helpers in 1.5.a exist solely to power the *negative* assertions in S10/S11 (canary stays OUT) coming in 1.5.b.

## Locked decisions

- **Test framework: vitest with sequential pool.** Already used everywhere in the workspace. The pool config forces single-fork execution (`pool: "forks", poolOptions: { forks: { singleFork: true } }`) so cluster-mutation tests don't conflict.
- **Subset-served signal: single response header from the immediate handler.** Each service stamps `x-served-version: stable | canary` on its outbound response. This is the simplest verifiable signal, costs essentially nothing in production, and is sufficient for any scenario where the test calls a service directly. Multi-hop chain verification is a 1.5.b concern.
- **Helpers shell out to `node tools/canary-ctl/bin/canary-ctl`** rather than importing canary-ctl's TS modules. Decouples the e2e harness from canary-ctl internals; the binary is the public contract.
- **Load generator: TS-native** (axios + interval pacing). No vegeta/k6 binary dep. Returns request stats; sufficient for the load characteristics in the spec (~50 rps for 30 s).

## Architecture

### TS e2e harness as a pnpm workspace package

`tests/e2e/` becomes a new pnpm workspace package `@canary/e2e` with its own `package.json`, `tsconfig.json`, and `vitest.config.ts`. Following the same shape as `tools/canary-ctl` from Plan 1.4. Adds `tests/*` to the pnpm-workspace.yaml glob.

**Why a workspace package and not just a sibling test directory:** scopes deps (kafkajs, axios) cleanly; lets `pnpm --filter @canary/e2e test` run scenarios in isolation; matches the workspace pattern.

### Vitest config

```typescript
// tests/e2e/vitest.config.ts
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["**/*.test.ts"],
    environment: "node",
    globals: false,
    testTimeout: 5 * 60_000,        // 5-minute per-test timeout
    hookTimeout: 5 * 60_000,        // 5-minute beforeAll/afterAll
    pool: "forks",
    poolOptions: {
      forks: { singleFork: true },  // sequential execution
    },
    fileParallelism: false,         // one scenario at a time
    reporters: ["verbose"],
  },
});
```

### Subset-served instrumentation

**`platform/lib-java/`** — new class:

`XCanaryResponseHeaderFilter` — a servlet `Filter` ordered AFTER the inbound `XCanaryRequestFilter`. Reads the `VERSION` env var (or Spring property `canary.version`) at startup; defaults to `"stable"` if unset. On every response, sets `x-served-version: <value>`. Wired by `XCanaryAutoConfiguration` as a Spring bean.

**`platform/lib-node/`** — new exported helper:

`xServedVersionMiddleware()` — an Express middleware factory. Reads `process.env.VERSION ?? "stable"` once at module load. Returns a middleware that calls `res.setHeader("x-served-version", value)` then `next()`. Each Node service adds it to its app setup with one line: `app.use(xServedVersionMiddleware())`.

**`deploy/helm/service-chart/templates/deployment.yaml`** — modify to add `VERSION` env var sourced from `.Values.version`:

```yaml
env:
  - name: VERSION
    value: {{ .Values.version | quote }}
  # ...existing env vars
```

`.Values.version` already exists (used for the pod label). Stable releases get `VERSION=stable`; canary releases get `VERSION=canary` (already in `canary-overlay.yaml`).

**Each service** (`services/*/`) — one-line addition for Node services to add the middleware to their app. Java services need no per-service change (auto-config wires it).

### Helpers

Organized by surface in `tests/e2e/helpers/`:

**`canary.ts`** — wraps `canary-ctl` via `node:child_process.execFile`. Exports:
- `deployCanary(svc: string, tag: string): Promise<void>` — throws on non-zero exit
- `rollback(svc: string): Promise<void>`
- `status(svc: string): Promise<CanaryStatus>` — invokes `canary-ctl status <svc> --json`, parses JSON
- `reconcile(svc: string, opts?: { adopt?: boolean }): Promise<void>`

**`traffic.ts`** — single-request driver. Exports:
- `sendOrder(opts: SendOrderOpts): Promise<{status, data, headers}>` — same shape as `tools/traffic-cli/src/index.ts:sendOrder()`. We re-implement here rather than importing to keep the e2e package independent of tool internals.

**`subset.ts`** — assertion helpers. Exports:
- `assertServedVersion(headers: Record<string, string>, expected: "stable" | "canary"): void` — throws if header missing or mismatched
- `getServedVersion(headers: Record<string, string>): "stable" | "canary" | null` — non-throwing accessor

**`kafka-admin.ts`** — kafkajs Admin client (deferred wiring; Plan 1.5.b uses it for S10). 1.5.a includes:
- `connect(): Promise<void>` and `disconnect(): Promise<void>` — manage admin client lifecycle
- `consumerGroupMembers(groupId: string): Promise<MemberInfo[]>` — fetches member list

The bootstrap broker is `localhost:9092` and reaches the kind cluster via the Strimzi external listener (or via `kubectl port-forward` started by the test setup). Plan 1.5.a sets up the import + types but only smoke-tests the connection in S1.

**`restate-admin.ts`** — axios client. Exports:
- `listDeployments(): Promise<RestateDeployment[]>` — GET `http://localhost:9070/deployments`

**`load.ts`** — TS-native load generator. Exports:
- `runLoad(opts: LoadOpts): Promise<LoadStats>` where:
  - `LoadOpts = { url, rps, durationSeconds, headers?, payload? }`
  - `LoadStats = { requestsSent, successCount, failureCount, p50Ms, p99Ms, totalDurationMs, errorSamples: string[] }`

Implementation uses `setInterval(() => fire(), 1000 / rps)`. Each `fire()` sends an axios request; on response, records latency. After `durationSeconds`, clears the interval, awaits in-flight requests, returns stats. Errors are sampled (max 5 in `errorSamples`).

### S1 Baseline scenario

```typescript
// tests/e2e/s1-baseline.test.ts
import { describe, it, expect, beforeAll } from "vitest";
import { sendOrder } from "./helpers/traffic.js";
import { assertServedVersion } from "./helpers/subset.js";
import { status } from "./helpers/canary.js";

const SERVICES = ["order-service", "payment-service", "inventory-service", "notification-service", "audit-service"] as const;

describe("S1 Baseline — all stable, no canaries deployed", () => {
  beforeAll(async () => {
    // Verify the cluster is in the all-stable starting state.
    for (const svc of SERVICES) {
      const s = await status(svc);
      if (s.helmCanaryPresent) {
        throw new Error(`Pre-condition failed: ${svc} has a canary release. Run \`make canary-rollback SVC=${svc}\` first.`);
      }
    }
  });

  it("GET /api/orders without x-canary returns 200 from stable", async () => {
    const r = await sendOrder({ canary: false, user: "s1-stable", sku: "sku-1", quantity: 1, amount: 100 });
    expect(r.status).toBeGreaterThanOrEqual(200);
    expect(r.status).toBeLessThan(300);
    assertServedVersion(r.headers, "stable");
  });

  it("GET /api/orders with x-canary returns 200 from stable (graceful fallback)", async () => {
    const r = await sendOrder({ canary: true, user: "s1-fallback", sku: "sku-1", quantity: 1, amount: 100 });
    expect(r.status).toBeGreaterThanOrEqual(200);
    expect(r.status).toBeLessThan(300);
    assertServedVersion(r.headers, "stable");
  });
});
```

The two `it` blocks exercise the harness fully:
- Helpers wiring (status, sendOrder, assertServedVersion) all run
- The cluster is hit with both flavors of request
- Both must return 2xx from stable (no canary deployed → graceful fallback via the absent VS header rule from Plan 1.4)
- The `x-served-version` header must come back from the order-service (the immediate handler — Node service)

The scenario also doubles as an inadvertent S5 (graceful fallback) coverage. S5 still gets its own dedicated file in 1.5.b for clarity; S1 here is the harness smoke test.

### Make targets

```makefile
e2e: ## Run e2e scenarios (use SCENARIO=<name> to run one)
	@pnpm --filter @canary/e2e build >/dev/null
	@if [ -n "$(SCENARIO)" ]; then \
	  pnpm --filter @canary/e2e test $(SCENARIO); \
	else \
	  pnpm --filter @canary/e2e test; \
	fi

ci-local: ## Run fast e2e subset (S1, S2, S5, S8, S9, S12 — only S1 in 1.5.a)
	@pnpm --filter @canary/e2e build >/dev/null
	@pnpm --filter @canary/e2e test s1
```

`make e2e` runs everything; `make e2e SCENARIO=s1` filters to one file. `make ci-local` runs only the curated fast subset (initially just S1).

### Repo additions

```
tests/e2e/                                  # NEW pnpm package: @canary/e2e
├── package.json                            # NEW
├── tsconfig.json                           # NEW
├── vitest.config.ts                        # NEW
├── helpers/
│   ├── canary.ts                           # NEW: canary-ctl shell-out wrapper
│   ├── traffic.ts                          # NEW: sendOrder
│   ├── subset.ts                           # NEW: x-served-version assertion
│   ├── kafka-admin.ts                      # NEW: kafkajs admin client
│   ├── restate-admin.ts                    # NEW: axios → :9070
│   └── load.ts                             # NEW: TS load generator
└── s1-baseline.test.ts                     # NEW: S1 scenario

platform/lib-java/                          # MODIFY
├── src/main/java/.../XCanaryResponseHeaderFilter.java  # NEW
├── src/main/java/.../XCanaryAutoConfiguration.java     # MODIFY: register the filter bean
└── src/test/.../XCanaryResponseHeaderFilterTest.java   # NEW

platform/lib-node/                          # MODIFY
├── src/x-served-version-middleware.ts      # NEW
├── src/index.ts                            # MODIFY: re-export the middleware
└── test/x-served-version-middleware.test.ts # NEW

services/order-service/src/                 # MODIFY: add app.use(xServedVersionMiddleware())
services/notification-service/src/          # MODIFY: same

deploy/helm/service-chart/templates/deployment.yaml  # MODIFY: add VERSION env var

pnpm-workspace.yaml                         # MODIFY: add tests/* glob
Makefile                                    # MODIFY: add e2e + ci-local targets
README.md                                   # MODIFY: add Plan 1.5.a section
```

## Data flow

### Single-request scenario (S1's "without x-canary" assertion)

1. Test calls `sendOrder({ canary: false, ... })`.
2. axios POSTs to `http://localhost:8080/api/orders` (the kind ingress NodePort from Plan 1.1) with no `x-canary` header.
3. Istio ingress evaluates `order-service`'s VS → header rule absent (no canary deployed) → default rule routes to stable subset.
4. order-service (stable) processes the request. `xCanaryMiddleware` sees no header (context flag stays false). `xServedVersionMiddleware` stamps `x-served-version: stable` on the response.
5. order-service calls inventory/payment/notification (all stable) — but those are inside the cluster; their response headers don't propagate back through the order-service's response to the test.
6. order-service responds 2xx with `x-served-version: stable`.
7. Test asserts: status 2xx, `x-served-version: stable`. ✅

### Single-request scenario (S1's "with x-canary" assertion)

1. Test calls `sendOrder({ canary: true, ... })`.
2. axios POSTs with `x-canary: true`.
3. Istio ingress evaluates VS → header rule absent → default rule → stable subset (graceful fallback).
4. order-service (stable) sees `x-canary: true`, propagates it to downstream calls (per Plan 1.2 lib). All downstreams also fall back to stable.
5. order-service responds 2xx with `x-served-version: stable`.
6. Test asserts: status 2xx, `x-served-version: stable`. ✅ (Demonstrates graceful fallback even with the header set.)

### Where multi-hop verification would fit

For scenarios where the test needs to verify *each hop* (S3, S4, S8 in 1.5.b), the simple response header is insufficient — only the immediate handler stamps it, and downstream service responses don't reach the test directly. Plan 1.5.b will choose between two mechanisms:

- **Per-hop header chain**: each service prepends `<svc>=<version>` to a `x-served-by-chain` header on its outbound HTTP response. Order-service sees its downstreams' chains, prepends own, returns. Test parses CSV.
- **Jaeger trace query**: each request's trace ID (from Istio's tracing headers) is captured at the edge; test queries the Jaeger API after the fact to walk spans and read each pod's `version` tag.

1.5.a defers this decision until 1.5.b's scenario implementation begins.

## Error handling

The e2e harness has its own failure modes distinct from Phase 1's substrate:

- **Cluster not reachable** (kind not running, port-forward not active): every helper throws an axios/connection error. Tests fail with clear messages. We do NOT auto-bootstrap the cluster — that's the operator's job (`make up && make deploy-services`). The S1 `beforeAll` does a status check on each service; failures here surface as "cluster not in expected state" rather than cryptic test failures.
- **Subset-served header missing**: `assertServedVersion` throws `expected x-served-version header but got: <list of headers received>`. Indicates the lib changes weren't applied — operator runs `make build-services && make build-images && make load-images && make deploy-services` to refresh.
- **canary-ctl shell-out failure**: helpers re-throw with the captured stderr included. Test failure messages will include the full canary-ctl error.
- **Test interrupted mid-scenario** (Ctrl-C): leaves the cluster in whatever state the scenario was in. The next `make e2e` run's `beforeAll` will detect the inconsistency and fail with a clear "rollback first" message. The operator runs `make canary-rollback SVC=<svc>` for each affected service.
- **Kafka admin connect failure**: helpers' `connect()` throws if `localhost:9092` isn't reachable. Plan 1.5.a's S1 doesn't actually use the admin client; the helper exists for 1.5.b.

## Testing strategy

### What 1.5.a tests itself

1. **Unit tests for the lib changes**:
   - `XCanaryResponseHeaderFilterTest` (Java): the filter sets the right header from env var; defaults to "stable" if env unset; doesn't override an existing header.
   - `xServedVersionMiddleware` test (Node): same three properties.
2. **Helper unit tests are deliberately omitted in 1.5.a.** The helpers are thin wrappers around well-tested deps (canary-ctl binary, axios, kafkajs); their correctness is verified by S1 actually running end-to-end against a real cluster. Mocked unit tests would re-test the wrappers without surfacing real failure modes (cluster misconfig, header missing, etc.). 1.5.b may add helper tests if specific bugs surface during scenario implementation.
3. **S1 Baseline** — the integration smoke against a real cluster. Pass = the entire pipeline (lib → service → ingress → response → header → assertion) works.

### What requires a real cluster

S1 itself plus operator manual verification. Same model as Plan 1.4: unit tests run anywhere; smoke/e2e tests need `make up && make deploy-services && make build-services && make build-images && make load-images && make deploy-services`.

### Continuous local verification

`make verify` (the existing target) runs all unit tests including the new lib tests. Adds ~10 tests; runtime change is negligible.

`make ci-local` (the new target) runs only S1 against a real cluster. ~30s once the cluster is up.

`make e2e` runs all e2e scenarios. In 1.5.a that's just S1. After 1.5.b: ~10–15 minutes for the full 13.

## Operator workflow (after Plan 1.5.a)

```
make up                                                 # 1.1
make build-services                                     # 1.3.a + new lib changes
make build-images && make load-images                   # 1.3.b
make deploy-services                                    # 1.3.b
make smoke-services                                     # 1.3.b
make smoke-canary                                       # 1.4

# 1.5.a additions:
make e2e SCENARIO=s1                                    # run S1 Baseline only
make e2e                                                # run all e2e scenarios (just S1 in 1.5.a)
make ci-local                                           # fast subset (just S1 in 1.5.a)
```

## Done when

- All unit tests pass: `pnpm --filter @canary/lib-node test`, `./gradlew :platform:lib-java:test`, plus the new `@canary/e2e` package's unit tests (none in 1.5.a — see Testing).
- `make verify` passes (with the new lib unit tests included).
- `make e2e SCENARIO=s1` passes against a fresh `make up && make deploy-services` cluster, with a re-built image tree (the lib changes need to be in the image).
- The Helm chart deploys with `VERSION` env var set; `kubectl exec` into a stable pod shows `VERSION=stable`; into a canary pod shows `VERSION=canary`.
- README has a `## Plan 1.5.a` section describing the harness and the operator workflow.

## Open assumptions

- The kind ingress NodePort (`http://localhost:8080`) is reachable from the host.
- Restate Admin (`http://localhost:9070`) is reachable from the host (via the NodePort from Plan 1.1).
- Kafka broker `localhost:9092` is reachable from the host. **Note:** Plan 1.1's Strimzi setup uses an internal listener only. Reaching it from the host requires either a `kubectl port-forward svc/my-cluster-kafka-bootstrap 9092:9092 -n kafka` started outside the test, OR adding an external listener to the Kafka CR. Plan 1.5.a's S1 does NOT use Kafka admin, so this gap is acceptable for now; 1.5.b will resolve it (likely by adding an external listener to the Kafka CR or by having the helper start a port-forward).
- The lib changes do not break any existing unit/integration tests (lib + service test suites should pass unchanged plus the new lib tests).
- The Helm chart's `.Values.version` is correctly set in both `values/<svc>.yaml` (defaults to `stable`) and `values/canary-overlay.yaml` (`canary`) — verified during 1.4 review; no changes needed in 1.5.a.
