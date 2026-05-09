# Canary Release Management — Phase 1.4 Design (`canary-ctl` + `traffic-cli`)

**Status:** Approved (awaiting user review)
**Date:** 2026-05-09
**Phase:** Phase 1, sub-plan 4 of 5
**Umbrella spec:** `docs/superpowers/specs/2026-05-08-canary-release-phase-1-design.md`

## Project context

Phase 1 of the canary release reference architecture has shipped its substrate: a kind cluster with Istio, Strimzi/Kafka, Restate, and observability addons (Plan 1.1); shared `lib-java` and `lib-node` libraries that propagate `x-canary: true` across HTTP, Kafka, and Restate boundaries (Plan 1.2); five domain services wired together with the right call graph (Plan 1.3.a); and stable-only deployments to the cluster behind Istio routing (Plan 1.3.b).

What is missing from Phase 1 is the actual canary release mechanism: a tool that creates and removes canary Deployments together with the per-service VirtualService header-match rule that routes `x-canary: true` requests to those Deployments, plus a small client to drive requests through the system with and without the header.

Plan 1.4 delivers `tools/canary-ctl` and `tools/traffic-cli`. Plan 1.5 will use these to implement the 13 canonical acceptance scenarios from the umbrella design.

## Goals

1. Implement `canary-ctl` as the single source of truth for the per-service canary lifecycle on the existing kind cluster, supporting four commands (`deploy-canary`, `rollback`, `status`, `reconcile`) with per-service state file and partial-state recovery.
2. Implement `traffic-cli` as a thin request driver that sends a single HTTP request to the kind ingress with or without the `x-canary: true` header and pretty-prints the response.
3. Have `canary-ctl` enforce the umbrella design's invariants: header rule and canary Deployment created and removed in lockstep; header rule first off on rollback; auto-rollback on rollout failure; idempotent rollback.
4. Ship a small bats smoke test that drives `canary-ctl deploy-canary` → traffic with header → `canary-ctl rollback` end-to-end against a fresh `make up && make deploy-services` cluster, plus unit tests for the non-cluster pieces.

## Non-goals (Plan 1.4)

- The 13 canonical acceptance scenarios — Plan 1.5.
- Load generators (Vegeta, k6) for S1/S2/S7 — Plan 1.5.
- Multi-service canary topologies as automated assertions — `canary-ctl` will be polymorphic over service, but Plan 1.5 owns the topology scenarios.
- Conversion of `canary-ctl` from a CLI with a state file to a Kubernetes controller — Phase 4.
- Percent-split routing — Phase 4.
- Auto-rollback on health regression after a successful deploy — Phase 4 (Argo Rollouts). Plan 1.4 only auto-rolls back on initial rollout failure (within `progressDeadlineSeconds`).
- OPA/Kyverno admission policies hardening canary labels — Phase 4.
- Reconcile as a continuous controller loop — Plan 1.4's `reconcile` is one-shot per invocation; the operator runs it on demand or after partial failures.

## Locked decisions

The following are settled before this design and not revisited here:

- **Implementation language: TypeScript on Node**, packaged as pnpm workspace packages. Same toolchain as `lib-node` and the Node services. The 1.5 e2e harness will also be TypeScript per the umbrella design, so all three tools share the same language.
- **Cluster mutation strategy: shell out to `helm` and `kubectl`.** No `@kubernetes/client-node`. This matches the existing `deploy/services/deploy.sh` pattern and keeps the dependency tree minimal. State management lives in TypeScript; cluster I/O is shell.
- **Service registry: hard-coded TS const.** Phase 1 has 5 known services; a config file is YAGNI. Adding a 6th service is a one-line PR.

## Architecture

### Single CLI binary, four commands

`canary-ctl` is one Node CLI with four subcommands. Each subcommand reads the service registry, takes one or more arguments, and either mutates cluster state through `helm`/`kubectl` shell-outs or reports current state.

| Command | Arguments | Effect |
|---|---|---|
| `deploy-canary <svc> <tag>` | service name; image tag (e.g. `v2`) | Creates the canary Helm release and inserts the header-match rule into the service's VirtualService. Auto-rolls back if the rollout fails inside `progressDeadlineSeconds` (120 s). |
| `rollback <svc>` | service name | Removes the header rule first, sleeps `--grace-seconds` (default 10 s), then deletes the canary Helm release, then clears the state file. Idempotent: works from any starting state. |
| `status <svc>` | service name | Prints the current canary state for the service: state-file phase, helm release presence, VS rule presence, ready replica count. `--json` for machine-readable. Reports drift (state file vs cluster reality) as a non-zero exit. |
| `reconcile <svc>` | service name | Inspects cluster + state file, decides fix-forward or roll back, applies the chosen action, leaves state consistent. One-shot. |

Global flags: `--namespace` (default `services`), `--grace-seconds` (default 10), `--state-dir` (default `~/.canary-ctl`), `--repo-root` (default `process.cwd()`), `--dry-run` (print what would be done; do not mutate).

### Service registry

`tools/canary-ctl/src/registry.ts`:

```typescript
export interface ServiceEntry {
  name: string;                  // logical service name, e.g. "payment-service"
  helmReleaseStable: string;     // existing release name from 1.3.b
  helmReleaseCanary: string;     // canary release name created by canary-ctl
  namespace: string;             // "services"
  virtualService: string;        // VS name; matches service name in 1.3.b
  valuesFile: string;            // path to deploy/helm/values/<svc>.yaml
  canaryOverlay: string;         // path to deploy/helm/values/canary-overlay.yaml
  chartPath: string;             // path to deploy/helm/service-chart
}

export const REGISTRY: Record<string, ServiceEntry> = {
  "payment-service": {
    name: "payment-service",
    helmReleaseStable: "payment-service",
    helmReleaseCanary: "payment-service-canary",
    namespace: "services",
    virtualService: "payment-service",
    valuesFile: "deploy/helm/values/payment-service.yaml",
    canaryOverlay: "deploy/helm/values/canary-overlay.yaml",
    chartPath: "deploy/helm/service-chart",
  },
  // ...four more entries with the same shape for order, inventory, notification, audit
};

export function lookup(svc: string): ServiceEntry { /* throws on unknown */ }
```

Stable release names come from Plan 1.3.b (one Helm release per service, release name == service name). Canary release names are `<svc>-canary`. The `lookup` helper throws a clear error on unknown service names. Paths are repo-relative; `canary-ctl` resolves them against `process.cwd()` (the operator must invoke `canary-ctl` from the repo root, or pass `--repo-root <path>`).

### State file

One file per service at `~/.canary-ctl/<service>.json`. Plain JSON. Created on first write. Removed on `rollback` and on `reconcile` if the cluster has no canary release.

```json
{
  "service": "payment-service",
  "phase": "active",
  "tag": "v2",
  "deployedAt": "2026-05-09T18:30:00Z"
}
```

**Phases.** A small finite state machine, written by `deploy-canary` and `rollback` after each successful step:

| Phase | Meaning | Created by | Cleared by |
|---|---|---|---|
| `deploying` | Helm install in progress; rollout not yet Ready | `deploy-canary` step 2 | progressing to `deployment-ready` or auto-rollback |
| `deployment-ready` | Helm release Ready; header rule not yet applied | `deploy-canary` step 5 | progressing to `active` or auto-rollback |
| `active` | Header rule in place; canary serving header-flagged requests | `deploy-canary` step 7 | `rollback` |
| `rolling-back` | Rollback in progress | `rollback` step 1 | rollback completion (state file deleted) |

Atomic writes use a temp-file + rename pattern. Read returns `null` if the file is absent.

### VirtualService header-rule lifecycle

The 1.3.b VirtualServices are default-only and stay that way at rest. `canary-ctl` mutates them via `kubectl patch --type merge`, replacing the entire `spec.http` array with either the 2-rule or 1-rule shape.

**Patch payload to add the header rule (apply on `deploy-canary` step 6):**

```json
{
  "spec": {
    "http": [
      {
        "name": "canary-by-header",
        "match": [{"headers": {"x-canary": {"exact": "true"}}}],
        "route": [{"destination": {"host": "<svc>", "subset": "canary"}}]
      },
      {
        "name": "default",
        "route": [{"destination": {"host": "<svc>", "subset": "stable"}}]
      }
    ]
  }
}
```

**Patch payload to remove the header rule (apply on `rollback` step 1):**

```json
{
  "spec": {
    "http": [
      {
        "name": "default",
        "route": [{"destination": {"host": "<svc>", "subset": "stable"}}]
      }
    ]
  }
}
```

JSON-merge semantics replace `spec.http` wholesale, so ordering is enforced by the payload itself. This satisfies the umbrella design's §G "VirtualService rule ordering" guarantee.

### Helm release for canary

`canary-ctl deploy-canary <svc> <tag>` runs:

```bash
helm upgrade --install <svc>-canary deploy/helm/service-chart \
  -n services \
  -f deploy/helm/values/<svc>.yaml \
  -f deploy/helm/values/canary-overlay.yaml \
  --set image.tag=<tag> \
  --wait --timeout 120s
```

The `canary-overlay.yaml` already (per 1.3.b) hard-codes `version: canary`, `KAFKA_CONSUMERS_ENABLED=false`, `RESTATE_REGISTER_HANDLERS=false`, and disables the post-install Restate registration Job. The 1.3.b `service-chart` already produces a separate Deployment named `<svc>-canary` when `version: canary` is set.

Rollback runs:

```bash
helm uninstall <svc>-canary -n services --wait
```

### `traffic-cli`

A separate tiny pnpm package `tools/traffic-cli`:

```
traffic-cli order [--canary] [--user u1] [--sku sku-1] [--quantity 1] [--amount 100] [--url http://localhost:8080]
```

Wraps a single `POST /api/orders` against the kind ingress (default `http://localhost:8080`, the NodePort from Plan 1.1). `--canary` adds the `x-canary: true` header. Prints status code, request headers sent, response body. That is all it does in 1.4 — no load-gen, no scenario logic, no subset-served assertions. Verifying which subset served belongs to Kiali, traces, and the e2e harness in Plan 1.5.

## Data flow

### `deploy-canary order-service v2` (happy path)

1. Read registry for `order-service`.
2. Write state `{phase: "deploying", tag: "v2"}`.
3. Run `helm upgrade --install order-service-canary ... --set image.tag=v2 --wait --timeout 120s`. Helm exits 0 once the canary Deployment reaches Ready or fails the rollout deadline.
4. Run `kubectl rollout status deploy/order-service-canary --timeout=120s` as a belt-and-suspenders check (in case helm's `--wait` semantics change).
5. Write state `{phase: "deployment-ready", tag: "v2"}`.
6. Run `kubectl patch virtualservice/order-service -n services --type merge -p '<2-rule payload>'`.
7. Write state `{phase: "active", tag: "v2", deployedAt: <now>}`.
8. Print success summary and exit 0.

### `deploy-canary order-service v2` (rollout fails)

1. Read registry.
2. Write state `{phase: "deploying"}`.
3. Run helm. Helm exits non-zero (e.g., bad image tag → ImagePullBackOff → rollout deadline).
4. Auto-rollback: write state `{phase: "rolling-back"}`. Apply the 1-rule patch unconditionally (no-op if header rule was never added). Run `helm uninstall order-service-canary --wait`. Delete state file.
5. Print failure summary (rollout failed, auto-rolled back) and exit non-zero.

### `rollback order-service` (active canary)

1. Read state. If `phase == "active"`, run the 1-rule patch to remove the header rule.
2. Sleep `--grace-seconds` (default 10 s) to let in-flight canary requests drain.
3. Run `helm uninstall order-service-canary --wait`.
4. Delete state file.
5. Print success and exit 0.

### `rollback order-service` (no active canary)

1. Read state — file absent. Run a `helm list` reality check; if no canary release, print "nothing to do" and exit 0. Idempotent.
2. If state file absent but canary release exists (orphan from a partial deploy that crashed before writing state), run helm uninstall and exit 0.

### `reconcile order-service`

Reads the (state-file × cluster) cross-product and fires the right repair:

| State file | Helm canary release | VS header rule present | Action |
|---|---|---|---|
| absent | absent | absent | clean state, no-op |
| absent | absent | present | drift: remove header rule, exit 0 (warn: orphaned VS rule with no canary) |
| absent | present | absent | adopt-or-rollback (`--adopt` flag → write state `active` and apply header rule; default → uninstall release) |
| absent | present | present | adopt-or-rollback (default → roll back) |
| `deploying` | absent | absent | rollout never started or already cleared — clear state, exit 0 |
| `deploying` | present (Ready) | absent | progress to `deployment-ready`, then to `active` (apply header rule), update state |
| `deploying` | present (NotReady) | absent | wait `--reconcile-timeout` (default 30 s, distinct from the 120 s deploy deadline); if still not Ready → roll back |
| `deployment-ready` | present | absent | apply header rule, write `active` |
| `deployment-ready` | present | present | drift: state lags reality — update state to `active` |
| `deployment-ready` | absent | * | drift: write missing rollback — clear header rule if present, clear state |
| `active` | present | present | already converged — no-op |
| `active` | present | absent | drift: re-apply header rule |
| `active` | absent | * | drift: clear header rule if present, clear state |
| `rolling-back` | * | * | finish the rollback: 1-rule patch, helm uninstall, clear state |

Reconcile is one-shot — a single command invocation, not a loop. The operator re-runs it if needed. Plan 1.4 ships a textual outcome line per (cluster × state) case so the operator can see what was decided.

### `status order-service`

Reads state file, runs `helm list` and `kubectl get virtualservice/order-service -o jsonpath`, prints:

```
order-service:
  state file: active (tag v2, deployed 2026-05-09T18:30:00Z)
  helm release order-service-canary: present, status deployed, ready 1/1
  virtualservice header rule: present (rule "canary-by-header" at index 0)
  drift: none
```

`--json` returns a structured object suitable for piping to `jq`. Drift detected → exit 2 (the operator should run `reconcile`).

## Error handling and fallbacks

Direct mapping from the umbrella design's "Error handling and fallbacks" section. Bracketed labels match those in the umbrella spec.

- **[A] Canary lifecycle failures.** Helm `--wait` fires `progressDeadlineSeconds: 120` (the `service-chart` deployment template already sets this from 1.3.b). On non-zero helm exit, `canary-ctl` enters auto-rollback: 1-rule patch (idempotent — no-op if rule never added), helm uninstall, state file deleted. Exit code is non-zero.
- **[B] Partial state.** State file written after each successful step (deploying → deployment-ready → active). `reconcile` table covers every (state × cluster) case.
- **[C] Subset and label contamination.** Mitigation is unchanged from 1.3.b: the canary overlay hard-codes `version: canary`. `canary-ctl` is the only path that creates canary Deployments. The Helm template itself enforces the label.
- **[F] Rollback (intentional).** Header rule first → grace sleep → helm uninstall → state cleared. The order matters and matches the spec exactly.
- **[G] VirtualService rule ordering.** The patch payload is the entire `spec.http` array, so ordering is enforced at apply time. There is no codepath in 1.4 that produces a misordered VS.

Failure modes specific to the tool itself:

- **`helm upgrade --install` succeeds but `kubectl rollout status` shows NotReady inside the timeout.** Treated as a rollout failure → auto-rollback.
- **`kubectl patch` fails after a successful helm install.** Treated as a deploy failure between phases `deployment-ready` and `active` → auto-rollback (header-rule patch was the failing step, so removing it is a no-op; helm uninstall + state delete proceed).
- **State file corrupt (invalid JSON).** `canary-ctl` fails with a clear error pointing at the file path; operator deletes it manually and runs `reconcile`. Plan 1.4 does not auto-repair corrupt state files — that's controller territory (Phase 4).
- **`canary-ctl` killed mid-step.** Next invocation sees a phase that does not match cluster reality; `reconcile` table fires the right repair.

## Testing strategy

Three layers, in order of how often they run:

### Unit tests

`tools/canary-ctl/test/` and `tools/traffic-cli/test/` using **vitest** (matches the Node service tests). No cluster needed. Mock the shell-out helpers.

Coverage target ~20 tests:

- `registry.ts` — lookup hit, lookup miss, lookup case-sensitivity (~3 tests).
- `state.ts` — read absent file, read existing file, atomic write, atomic write over existing, all 4 phases roundtrip (~6 tests).
- `kubectl.ts` patch payload generation — 2-rule and 1-rule payloads match the documented JSON byte-for-byte (~2 tests).
- `commands/deploy-canary.ts` — happy path with mocked helm/kubectl; helm-failure path triggers rollback; kubectl-patch-failure triggers rollback (~3 tests).
- `commands/rollback.ts` — active state path; absent state path (no-op); orphan release path (~3 tests).
- `commands/reconcile.ts` — one test per row of the reconcile decision table (~13 tests but condensed into 6–8 representative cases).
- `commands/status.ts` — drift detection (~2 tests).
- `traffic-cli` — happy path POST, --canary header attached, error reporting (~3 tests).

### Smoke test (`tests/canary/canary-ctl.bats`)

Runs against a real `make up && make build-services && make build-images && make load-images && make deploy-services` cluster. Five bats assertions:

1. `canary-ctl status payment-service` on a clean cluster reports no canary, no drift, exit 0.
2. `canary-ctl deploy-canary payment-service dev` (re-using the existing `canary-release-mgmt/payment-service:dev` image from 1.3.b with the `version: canary` overlay) succeeds; pod becomes Ready; header rule present in VS; state file has phase `active`.
3. `traffic-cli order --canary` returns 2xx (the assertion is just that traffic flows end-to-end with the header; verifying which subset *served* the payment hop belongs to Plan 1.5).
4. `canary-ctl rollback payment-service` removes the header rule, uninstalls the release, clears the state file; `status` reports clean.
5. `canary-ctl deploy-canary payment-service nope` (image tag `nope` does not exist → ImagePullBackOff → rollout deadline) auto-rolls back inside 120 s; final `status` reports clean; the deploy command exit code is non-zero.

The smoke test runs once via `make smoke-canary`. It is slower than the 1.3.b `make smoke-services` test (one full canary lifecycle takes 60–180 s) but is the only realistic way to prove the tool actually works against the cluster.

### Manual verification

Documented in the README's Plan 1.4 section. The operator runs:

```
canary-ctl deploy-canary payment-service stable
traffic-cli order --canary --quantity 2
# inspect Kiali traffic graph at http://localhost:20001 — confirms canary subset received the request
canary-ctl rollback payment-service
```

This is what the 1.5 e2e harness will automate. In 1.4 the operator confirms by eye.

## Repo additions

```
tools/
├── canary-ctl/
│   ├── package.json                # name: @canary/canary-ctl
│   ├── tsconfig.json
│   ├── bin/canary-ctl              # node shim: require('../dist/index.js')
│   ├── src/
│   │   ├── index.ts                # commander entrypoint; wires subcommands
│   │   ├── registry.ts             # 5-service constant map + lookup helper
│   │   ├── state.ts                # JSON state-file read/write with atomic semantics
│   │   ├── kubectl.ts              # shell-out: patch, rollout-status, get-vs, get-deploy
│   │   ├── helm.ts                 # shell-out: upgrade-install, uninstall, list
│   │   └── commands/
│   │       ├── deploy-canary.ts
│   │       ├── rollback.ts
│   │       ├── status.ts
│   │       └── reconcile.ts
│   └── test/                       # vitest unit tests
│       ├── registry.test.ts
│       ├── state.test.ts
│       ├── kubectl.test.ts
│       ├── deploy-canary.test.ts
│       ├── rollback.test.ts
│       ├── reconcile.test.ts
│       └── status.test.ts
└── traffic-cli/
    ├── package.json                # name: @canary/traffic-cli
    ├── tsconfig.json
    ├── bin/traffic-cli
    ├── src/index.ts                # commander entrypoint + axios POST
    └── test/index.test.ts

tests/canary/
└── canary-ctl.bats                 # 5 smoke assertions

deploy/routing/virtual-services/    # unchanged: still default-only baselines
                                    # canary-ctl mutates these in place at runtime
```

## Operator workflow (after Plan 1.4)

```
make up                                 # 1.1: bootstrap cluster
make build-services                     # 1.3.a: compile binaries
make build-images && make load-images   # 1.3.b: load to kind
make deploy-services                    # 1.3.b: install stable releases
make smoke-services                     # 1.3.b: assert stable substrate

# 1.4 additions:
canary-ctl deploy-canary payment-service v2
canary-ctl status payment-service
traffic-cli order --canary
canary-ctl rollback payment-service
canary-ctl reconcile order-service       # if the operator suspects drift
make smoke-canary                        # end-to-end bats assertion
```

`canary-ctl` and `traffic-cli` are shipped as pnpm workspace packages. The README documents both `pnpm --filter @canary/canary-ctl exec canary-ctl ...` and a recommended `pnpm link` alias for ergonomic local use.

## Done when

- All unit tests pass: `pnpm --filter @canary/canary-ctl test` and `pnpm --filter @canary/traffic-cli test`.
- `make verify` (the full project test target) passes.
- `make smoke-canary` passes against a fresh cluster.
- Operator can run the manual verification flow and confirm via Kiali that header-flagged requests hit the canary subset.
- README has a `## Plan 1.4` section listing the four `canary-ctl` commands and the `traffic-cli` invocation.

## Open assumptions

- The kind cluster has the Plan 1.3.b stable releases already installed before any `canary-ctl` invocation. `canary-ctl` does not bootstrap services — it only manages the canary side.
- The `canary-overlay.yaml` from 1.3.b is correct and complete (verified by the audit). Plan 1.4 consumes it as-is.
- The Helm `service-chart` produces a Deployment named `<svc>-canary` when `version: canary` is set (also verified by the audit — the deployment template uses `.Values.version` in the resource name).
- The kind ingress NodePort (`http://localhost:8080`) from Plan 1.1 is reachable from the host. `traffic-cli` defaults to that URL.
