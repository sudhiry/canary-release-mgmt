# Phase 1.4 — `canary-ctl` + `traffic-cli` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `tools/canary-ctl` (4-command CLI that owns the per-service canary lifecycle: Helm release + VirtualService header rule + state file) and `tools/traffic-cli` (single-request driver with optional `x-canary: true` header), proven by unit tests + a 5-assertion bats smoke test against a real kind cluster.

**Architecture:** Two pnpm workspace packages under `tools/`. `canary-ctl` shells out to `helm` and `kubectl` for cluster mutations; state lives in per-service JSON files at `~/.canary-ctl/<svc>.json`. Hard-coded 5-service registry. `traffic-cli` is a thin axios POST wrapper. Both built with TypeScript + commander; tests with vitest matching the existing Node service style.

**Tech Stack:**
- TypeScript 5.x (matches `lib-node` and Node services)
- pnpm workspaces (already configured in repo root)
- commander v12 (CLI parsing)
- axios v1.x (already a transitive dep via `lib-node`)
- vitest (already used in `lib-node` and Node services)
- bats-core 1.x (already used in `tests/infra/` and `tests/services/`)
- Node `node:child_process.execFile` (shell-out)
- `helm` 3.x and `kubectl` (assumed installed; not bundled)

**Spec reference:** `docs/superpowers/specs/2026-05-09-canary-release-phase-1-4-design.md`

---

## Prerequisites

The plan assumes:
- The Plan 1.1 substrate (`make up`) is reachable.
- The Plan 1.3.b stable releases are deployed (`make deploy-services`).
- `helm` and `kubectl` are on `PATH` and pointing at the kind cluster (`kubectl config current-context` returns `kind-canary-release-mgmt`).
- Repo-root pnpm workspace + Node 25 toolchain (existing).

---

## File Structure

```
tools/                                              # NEW directory
├── canary-ctl/
│   ├── package.json                                # NEW: name "@canary/canary-ctl"
│   ├── tsconfig.json                               # NEW
│   ├── vitest.config.ts                            # NEW
│   ├── bin/canary-ctl                              # NEW: node shim
│   ├── src/
│   │   ├── index.ts                                # NEW: commander entrypoint
│   │   ├── registry.ts                             # NEW: 5-service const map + lookup
│   │   ├── state.ts                                # NEW: JSON state file read/write
│   │   ├── kubectl.ts                              # NEW: shell-out helpers
│   │   ├── helm.ts                                 # NEW: shell-out helpers
│   │   ├── exec.ts                                 # NEW: execFile wrapper (mockable)
│   │   ├── logger.ts                               # NEW: small structured logger
│   │   └── commands/
│   │       ├── deploy-canary.ts                    # NEW
│   │       ├── rollback.ts                         # NEW
│   │       ├── status.ts                           # NEW
│   │       └── reconcile.ts                        # NEW
│   └── test/
│       ├── registry.test.ts                        # NEW
│       ├── state.test.ts                           # NEW
│       ├── kubectl.test.ts                         # NEW (patch payload generation)
│       ├── helm.test.ts                            # NEW (command-arg shape)
│       ├── deploy-canary.test.ts                   # NEW
│       ├── rollback.test.ts                        # NEW
│       ├── status.test.ts                          # NEW
│       └── reconcile.test.ts                       # NEW
├── traffic-cli/
│   ├── package.json                                # NEW: name "@canary/traffic-cli"
│   ├── tsconfig.json                               # NEW
│   ├── vitest.config.ts                            # NEW
│   ├── bin/traffic-cli                             # NEW
│   ├── src/index.ts                                # NEW: commander + axios POST
│   └── test/index.test.ts                          # NEW

tests/canary/
└── canary-ctl.bats                                 # NEW: 5 smoke assertions

pnpm-workspace.yaml                                 # MODIFY: add tools/* glob
package.json                                        # MODIFY: add helper scripts (optional)
Makefile                                            # MODIFY: add canary-* + smoke-canary targets
README.md                                           # MODIFY: add Plan 1.4 section
docs/superpowers/specs/2026-05-09-canary-release-phase-1-4-design.md  # already committed
```

**Why one package per tool:** keeps each tool's `bin` script simple, avoids name collisions, and lets us run `pnpm --filter @canary/canary-ctl test` in CI without dragging traffic-cli into the same test run. Same idiom as `lib-node`.

**Why a separate `exec.ts` wrapper:** the unit tests mock shell-out by mocking this single module. Keeps `kubectl.ts` and `helm.ts` test-friendly without each having to build a fixture.

**Why a small `logger.ts`:** structured one-line outputs (phase transitions, shell-out commands, decisions) instead of bare `console.log`. Easy to add `--verbose` later. ~30 lines, no deps.

---

## Task 1: Scaffold `tools/canary-ctl` and `tools/traffic-cli` packages

**Files:**
- Create: `tools/canary-ctl/package.json`
- Create: `tools/canary-ctl/tsconfig.json`
- Create: `tools/canary-ctl/vitest.config.ts`
- Create: `tools/canary-ctl/bin/canary-ctl`
- Create: `tools/canary-ctl/src/index.ts` (placeholder)
- Create: `tools/traffic-cli/package.json`
- Create: `tools/traffic-cli/tsconfig.json`
- Create: `tools/traffic-cli/vitest.config.ts`
- Create: `tools/traffic-cli/bin/traffic-cli`
- Create: `tools/traffic-cli/src/index.ts` (placeholder)
- Modify: `pnpm-workspace.yaml`

- [ ] **Step 1: Add `tools/*` to the workspace glob**

Read the existing `pnpm-workspace.yaml`. Add `'tools/*'` to the `packages:` list (next to whatever globs are already there). The file should end up listing both `platform/*` and `tools/*` (plus services/*, restate-defs-* etc. as already present).

- [ ] **Step 2: Write `tools/canary-ctl/package.json`**

```json
{
  "name": "@canary/canary-ctl",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "bin": {
    "canary-ctl": "bin/canary-ctl"
  },
  "main": "dist/index.js",
  "types": "dist/index.d.ts",
  "files": ["dist", "bin"],
  "scripts": {
    "build": "tsc -b",
    "test": "vitest run",
    "test:watch": "vitest",
    "lint": "tsc --noEmit"
  },
  "dependencies": {
    "commander": "^12.1.0"
  },
  "devDependencies": {
    "@types/node": "^22.0.0",
    "typescript": "^5.5.0",
    "vitest": "^2.0.0"
  }
}
```

- [ ] **Step 3: Write `tools/canary-ctl/tsconfig.json`**

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ES2022",
    "moduleResolution": "bundler",
    "lib": ["ES2022"],
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true,
    "declaration": true,
    "outDir": "dist",
    "rootDir": "src",
    "composite": true
  },
  "include": ["src/**/*.ts"],
  "exclude": ["dist", "node_modules", "test"]
}
```

- [ ] **Step 4: Write `tools/canary-ctl/vitest.config.ts`**

```typescript
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    environment: "node",
    globals: false,
  },
});
```

- [ ] **Step 5: Write `tools/canary-ctl/bin/canary-ctl`**

```bash
#!/usr/bin/env node
import("../dist/index.js").then(({ run }) => run(process.argv));
```

Make it executable: `chmod +x tools/canary-ctl/bin/canary-ctl`.

- [ ] **Step 6: Write `tools/canary-ctl/src/index.ts` (placeholder)**

```typescript
export function run(argv: string[]): Promise<void> {
  console.error("canary-ctl: not yet implemented");
  console.error("argv:", argv.slice(2).join(" "));
  return Promise.resolve();
}
```

- [ ] **Step 7: Repeat Steps 2–6 for `tools/traffic-cli` with name `@canary/traffic-cli`**

`package.json` (same shape; bin name `traffic-cli`; add `"axios": "^1.7.0"` to deps):

```json
{
  "name": "@canary/traffic-cli",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "bin": {
    "traffic-cli": "bin/traffic-cli"
  },
  "main": "dist/index.js",
  "types": "dist/index.d.ts",
  "files": ["dist", "bin"],
  "scripts": {
    "build": "tsc -b",
    "test": "vitest run",
    "test:watch": "vitest",
    "lint": "tsc --noEmit"
  },
  "dependencies": {
    "axios": "^1.7.0",
    "commander": "^12.1.0"
  },
  "devDependencies": {
    "@types/node": "^22.0.0",
    "typescript": "^5.5.0",
    "vitest": "^2.0.0"
  }
}
```

`tsconfig.json` and `vitest.config.ts` identical to `canary-ctl`'s. `bin/traffic-cli` shim identical pattern. `src/index.ts` placeholder identical.

- [ ] **Step 8: Install dependencies**

Run from the repo root:

```bash
pnpm install
```

Expected: pnpm picks up the new workspace globs, installs `commander`, `axios`, and devDeps. No errors.

- [ ] **Step 9: Build both packages to verify TS compiles**

```bash
pnpm --filter @canary/canary-ctl build
pnpm --filter @canary/traffic-cli build
```

Expected: both produce `dist/index.js`. No type errors.

- [ ] **Step 10: Commit**

```bash
git add pnpm-workspace.yaml pnpm-lock.yaml tools/canary-ctl tools/traffic-cli
git commit -m "feat(tools): scaffold canary-ctl + traffic-cli pnpm packages"
```

---

## Task 2: Service registry

**Files:**
- Create: `tools/canary-ctl/src/registry.ts`
- Create: `tools/canary-ctl/test/registry.test.ts`

- [ ] **Step 1: Write the failing test**

`tools/canary-ctl/test/registry.test.ts`:

```typescript
import { describe, expect, it } from "vitest";
import { lookup, REGISTRY, type ServiceEntry } from "../src/registry.js";

describe("registry", () => {
  it("contains all 5 Phase 1 services", () => {
    expect(Object.keys(REGISTRY).sort()).toEqual([
      "audit-service",
      "inventory-service",
      "notification-service",
      "order-service",
      "payment-service",
    ]);
  });

  it("each entry has the expected shape", () => {
    for (const [name, entry] of Object.entries(REGISTRY)) {
      expect(entry.name).toBe(name);
      expect(entry.helmReleaseStable).toBe(name);
      expect(entry.helmReleaseCanary).toBe(`${name}-canary`);
      expect(entry.namespace).toBe("services");
      expect(entry.virtualService).toBe(name);
      expect(entry.valuesFile).toBe(`deploy/helm/values/${name}.yaml`);
      expect(entry.canaryOverlay).toBe("deploy/helm/values/canary-overlay.yaml");
      expect(entry.chartPath).toBe("deploy/helm/service-chart");
    }
  });

  it("lookup returns the entry for a known service", () => {
    const e: ServiceEntry = lookup("payment-service");
    expect(e.helmReleaseCanary).toBe("payment-service-canary");
  });

  it("lookup throws for an unknown service", () => {
    expect(() => lookup("not-a-real-service")).toThrow(
      /unknown service: not-a-real-service/i,
    );
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
pnpm --filter @canary/canary-ctl test
```

Expected: FAIL — `registry.js` does not exist.

- [ ] **Step 3: Write the implementation**

`tools/canary-ctl/src/registry.ts`:

```typescript
export interface ServiceEntry {
  name: string;
  helmReleaseStable: string;
  helmReleaseCanary: string;
  namespace: string;
  virtualService: string;
  valuesFile: string;
  canaryOverlay: string;
  chartPath: string;
}

const SERVICES = [
  "audit-service",
  "inventory-service",
  "notification-service",
  "order-service",
  "payment-service",
] as const;

function entry(name: string): ServiceEntry {
  return {
    name,
    helmReleaseStable: name,
    helmReleaseCanary: `${name}-canary`,
    namespace: "services",
    virtualService: name,
    valuesFile: `deploy/helm/values/${name}.yaml`,
    canaryOverlay: "deploy/helm/values/canary-overlay.yaml",
    chartPath: "deploy/helm/service-chart",
  };
}

export const REGISTRY: Record<string, ServiceEntry> = Object.fromEntries(
  SERVICES.map((s) => [s, entry(s)]),
);

export function lookup(svc: string): ServiceEntry {
  const e = REGISTRY[svc];
  if (!e) {
    const known = Object.keys(REGISTRY).sort().join(", ");
    throw new Error(`unknown service: ${svc} (known: ${known})`);
  }
  return e;
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
pnpm --filter @canary/canary-ctl test
```

Expected: 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/canary-ctl/src/registry.ts tools/canary-ctl/test/registry.test.ts
git commit -m "feat(canary-ctl): hard-coded 5-service registry with lookup helper"
```

---

## Task 3: State file module

**Files:**
- Create: `tools/canary-ctl/src/state.ts`
- Create: `tools/canary-ctl/test/state.test.ts`

- [ ] **Step 1: Write the failing test**

`tools/canary-ctl/test/state.test.ts`:

```typescript
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { mkdtempSync, rmSync, existsSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type CanaryState, readState, writeState, deleteState } from "../src/state.js";

describe("state", () => {
  let dir: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "canary-ctl-state-"));
  });

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  it("readState returns null when the file is absent", () => {
    expect(readState(dir, "payment-service")).toBeNull();
  });

  it("writeState then readState roundtrips", () => {
    const s: CanaryState = {
      service: "payment-service",
      phase: "active",
      tag: "v2",
      deployedAt: "2026-05-09T18:30:00Z",
    };
    writeState(dir, s);
    expect(readState(dir, "payment-service")).toEqual(s);
  });

  it("writeState is atomic (no partial file on rename)", () => {
    const s: CanaryState = {
      service: "order-service",
      phase: "deploying",
      tag: "v1",
      deployedAt: "2026-05-09T18:30:00Z",
    };
    writeState(dir, s);
    const path = join(dir, "order-service.json");
    expect(existsSync(path)).toBe(true);
    expect(existsSync(`${path}.tmp`)).toBe(false);
  });

  it("deleteState removes the file (idempotent if absent)", () => {
    const s: CanaryState = {
      service: "audit-service",
      phase: "active",
      tag: "v1",
      deployedAt: "2026-05-09T18:30:00Z",
    };
    writeState(dir, s);
    deleteState(dir, "audit-service");
    expect(readState(dir, "audit-service")).toBeNull();
    // Idempotent: second call does not throw.
    expect(() => deleteState(dir, "audit-service")).not.toThrow();
  });

  it("readState throws a clear error on invalid JSON", () => {
    const path = join(dir, "broken.json");
    writeFileSync(path, "{ this is not json", "utf8");
    expect(() => readState(dir, "broken")).toThrow(/invalid state file/i);
  });

  it("each phase value roundtrips", () => {
    for (const phase of ["deploying", "deployment-ready", "active", "rolling-back"] as const) {
      const s: CanaryState = {
        service: "inventory-service",
        phase,
        tag: "v1",
        deployedAt: "2026-05-09T18:30:00Z",
      };
      writeState(dir, s);
      expect(readState(dir, "inventory-service")?.phase).toBe(phase);
    }
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
pnpm --filter @canary/canary-ctl test
```

Expected: FAIL — `state.js` does not exist.

- [ ] **Step 3: Write the implementation**

`tools/canary-ctl/src/state.ts`:

```typescript
import { existsSync, mkdirSync, readFileSync, renameSync, unlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";

export type CanaryPhase = "deploying" | "deployment-ready" | "active" | "rolling-back";

export interface CanaryState {
  service: string;
  phase: CanaryPhase;
  tag: string;
  deployedAt: string;
}

function pathFor(dir: string, svc: string): string {
  return join(dir, `${svc}.json`);
}

export function readState(dir: string, svc: string): CanaryState | null {
  const p = pathFor(dir, svc);
  if (!existsSync(p)) return null;
  const raw = readFileSync(p, "utf8");
  try {
    return JSON.parse(raw) as CanaryState;
  } catch (e) {
    throw new Error(`invalid state file at ${p}: ${(e as Error).message}`);
  }
}

export function writeState(dir: string, state: CanaryState): void {
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  const target = pathFor(dir, state.service);
  const tmp = `${target}.tmp`;
  writeFileSync(tmp, JSON.stringify(state, null, 2), "utf8");
  renameSync(tmp, target);
}

export function deleteState(dir: string, svc: string): void {
  const p = pathFor(dir, svc);
  if (!existsSync(p)) return;
  unlinkSync(p);
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
pnpm --filter @canary/canary-ctl test
```

Expected: all state tests PASS (registry tests still PASS).

- [ ] **Step 5: Commit**

```bash
git add tools/canary-ctl/src/state.ts tools/canary-ctl/test/state.test.ts
git commit -m "feat(canary-ctl): per-service JSON state file with atomic writes"
```

---

## Task 4: Shell-out wrapper (`exec.ts`)

**Files:**
- Create: `tools/canary-ctl/src/exec.ts`
- Create: `tools/canary-ctl/src/logger.ts`

This task creates the single point of indirection that all `helm.ts` / `kubectl.ts` shell-outs go through. Tests for `helm.ts` and `kubectl.ts` mock this module.

- [ ] **Step 1: Write `tools/canary-ctl/src/logger.ts`**

```typescript
type Level = "info" | "warn" | "error" | "debug";

let verbose = false;

export function setVerbose(v: boolean): void {
  verbose = v;
}

export function log(level: Level, msg: string, fields?: Record<string, unknown>): void {
  if (level === "debug" && !verbose) return;
  const stamp = new Date().toISOString();
  const f = fields ? " " + Object.entries(fields).map(([k, v]) => `${k}=${JSON.stringify(v)}`).join(" ") : "";
  // Always to stderr so stdout stays clean for `--json` outputs.
  process.stderr.write(`${stamp} [${level}] ${msg}${f}\n`);
}
```

- [ ] **Step 2: Write `tools/canary-ctl/src/exec.ts`**

```typescript
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { log } from "./logger.js";

const execFileAsync = promisify(execFile);

export interface RunResult {
  stdout: string;
  stderr: string;
}

export interface RunOptions {
  cwd?: string;
  timeoutMs?: number;
  /** If true, do not throw on non-zero exit; return result with stderr filled. */
  ignoreError?: boolean;
}

export async function run(cmd: string, args: string[], opts: RunOptions = {}): Promise<RunResult> {
  log("debug", "exec", { cmd, args, cwd: opts.cwd });
  try {
    const { stdout, stderr } = await execFileAsync(cmd, args, {
      cwd: opts.cwd,
      timeout: opts.timeoutMs,
      maxBuffer: 16 * 1024 * 1024,
    });
    return { stdout: stdout.toString(), stderr: stderr.toString() };
  } catch (err) {
    const e = err as NodeJS.ErrnoException & { stdout?: Buffer | string; stderr?: Buffer | string; code?: number | string };
    if (opts.ignoreError) {
      return {
        stdout: e.stdout?.toString() ?? "",
        stderr: e.stderr?.toString() ?? e.message,
      };
    }
    const stderr = e.stderr?.toString() ?? "";
    throw new Error(`${cmd} ${args.join(" ")} failed (exit ${e.code}): ${stderr || e.message}`);
  }
}
```

- [ ] **Step 3: No test for this task — covered by helm.ts and kubectl.ts tests via mocking**

- [ ] **Step 4: Verify TS compiles**

```bash
pnpm --filter @canary/canary-ctl build
```

Expected: no type errors.

- [ ] **Step 5: Commit**

```bash
git add tools/canary-ctl/src/exec.ts tools/canary-ctl/src/logger.ts
git commit -m "feat(canary-ctl): exec helper + structured stderr logger"
```

---

## Task 5: kubectl helpers + patch-payload tests

**Files:**
- Create: `tools/canary-ctl/src/kubectl.ts`
- Create: `tools/canary-ctl/test/kubectl.test.ts`

- [ ] **Step 1: Write the failing test**

`tools/canary-ctl/test/kubectl.test.ts`:

```typescript
import { describe, expect, it, vi } from "vitest";

vi.mock("../src/exec.js", () => ({
  run: vi.fn(async () => ({ stdout: "", stderr: "" })),
}));

import { run } from "../src/exec.js";
import {
  buildHeaderRulePatch,
  buildDefaultOnlyPatch,
  patchVirtualService,
  rolloutStatus,
  getDeploymentReady,
} from "../src/kubectl.js";

const runMock = vi.mocked(run);

describe("kubectl patch payloads", () => {
  it("buildHeaderRulePatch produces the 2-rule shape with header rule first", () => {
    const patch = JSON.parse(buildHeaderRulePatch("payment-service"));
    expect(patch).toEqual({
      spec: {
        http: [
          {
            name: "canary-by-header",
            match: [{ headers: { "x-canary": { exact: "true" } } }],
            route: [{ destination: { host: "payment-service", subset: "canary" } }],
          },
          {
            name: "default",
            route: [{ destination: { host: "payment-service", subset: "stable" } }],
          },
        ],
      },
    });
  });

  it("buildDefaultOnlyPatch produces the 1-rule shape", () => {
    const patch = JSON.parse(buildDefaultOnlyPatch("order-service"));
    expect(patch).toEqual({
      spec: {
        http: [
          {
            name: "default",
            route: [{ destination: { host: "order-service", subset: "stable" } }],
          },
        ],
      },
    });
  });
});

describe("kubectl shell-outs", () => {
  it("patchVirtualService runs kubectl patch with --type merge", async () => {
    runMock.mockResolvedValue({ stdout: "", stderr: "" });
    await patchVirtualService("payment-service", "services", '{"spec":{"http":[]}}');
    expect(runMock).toHaveBeenCalledWith(
      "kubectl",
      ["patch", "virtualservice", "payment-service", "-n", "services", "--type", "merge", "-p", '{"spec":{"http":[]}}'],
      expect.any(Object),
    );
  });

  it("rolloutStatus runs kubectl rollout status with timeout", async () => {
    runMock.mockResolvedValue({ stdout: "deployment \"payment-service-canary\" successfully rolled out\n", stderr: "" });
    await rolloutStatus("payment-service-canary", "services", 120);
    expect(runMock).toHaveBeenCalledWith(
      "kubectl",
      ["rollout", "status", "deployment/payment-service-canary", "-n", "services", "--timeout=120s"],
      expect.any(Object),
    );
  });

  it("getDeploymentReady reports ready=N/M", async () => {
    runMock.mockResolvedValue({
      stdout: JSON.stringify({ status: { readyReplicas: 1, replicas: 1 } }),
      stderr: "",
    });
    const r = await getDeploymentReady("payment-service-canary", "services");
    expect(r).toEqual({ ready: 1, total: 1, exists: true });
  });

  it("getDeploymentReady reports exists:false on NotFound", async () => {
    runMock.mockResolvedValue({ stdout: "", stderr: 'Error from server (NotFound): deployments.apps "x" not found' });
    const r = await getDeploymentReady("nope-canary", "services");
    expect(r).toEqual({ ready: 0, total: 0, exists: false });
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
pnpm --filter @canary/canary-ctl test
```

Expected: FAIL — `kubectl.js` does not exist.

- [ ] **Step 3: Write the implementation**

`tools/canary-ctl/src/kubectl.ts`:

```typescript
import { run } from "./exec.js";

export function buildHeaderRulePatch(svc: string): string {
  return JSON.stringify({
    spec: {
      http: [
        {
          name: "canary-by-header",
          match: [{ headers: { "x-canary": { exact: "true" } } }],
          route: [{ destination: { host: svc, subset: "canary" } }],
        },
        {
          name: "default",
          route: [{ destination: { host: svc, subset: "stable" } }],
        },
      ],
    },
  });
}

export function buildDefaultOnlyPatch(svc: string): string {
  return JSON.stringify({
    spec: {
      http: [
        {
          name: "default",
          route: [{ destination: { host: svc, subset: "stable" } }],
        },
      ],
    },
  });
}

export async function patchVirtualService(name: string, namespace: string, patch: string): Promise<void> {
  await run("kubectl", ["patch", "virtualservice", name, "-n", namespace, "--type", "merge", "-p", patch], {});
}

export async function rolloutStatus(deploy: string, namespace: string, timeoutSeconds: number): Promise<void> {
  await run(
    "kubectl",
    ["rollout", "status", `deployment/${deploy}`, "-n", namespace, `--timeout=${timeoutSeconds}s`],
    { timeoutMs: (timeoutSeconds + 5) * 1000 },
  );
}

export interface DeploymentReady {
  ready: number;
  total: number;
  exists: boolean;
}

export async function getDeploymentReady(name: string, namespace: string): Promise<DeploymentReady> {
  const r = await run(
    "kubectl",
    ["get", "deployment", name, "-n", namespace, "-o", "json"],
    { ignoreError: true },
  );
  if (r.stderr.includes("NotFound") || /not found/i.test(r.stderr)) {
    return { ready: 0, total: 0, exists: false };
  }
  const obj = JSON.parse(r.stdout) as { status?: { readyReplicas?: number; replicas?: number } };
  return {
    ready: obj.status?.readyReplicas ?? 0,
    total: obj.status?.replicas ?? 0,
    exists: true,
  };
}

export interface VirtualServiceRules {
  hasHeaderRule: boolean;
  ruleNames: string[];
}

export async function getVirtualServiceRules(name: string, namespace: string): Promise<VirtualServiceRules> {
  const r = await run(
    "kubectl",
    ["get", "virtualservice", name, "-n", namespace, "-o", "json"],
    { ignoreError: true },
  );
  if (r.stderr.includes("NotFound")) {
    return { hasHeaderRule: false, ruleNames: [] };
  }
  const obj = JSON.parse(r.stdout) as { spec?: { http?: Array<{ name?: string }> } };
  const names = (obj.spec?.http ?? []).map((h) => h.name ?? "");
  return {
    hasHeaderRule: names.includes("canary-by-header"),
    ruleNames: names,
  };
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
pnpm --filter @canary/canary-ctl test
```

Expected: all kubectl tests PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/canary-ctl/src/kubectl.ts tools/canary-ctl/test/kubectl.test.ts
git commit -m "feat(canary-ctl): kubectl shell-out helpers + JSON patch payload builders"
```

---

## Task 6: helm helpers

**Files:**
- Create: `tools/canary-ctl/src/helm.ts`
- Create: `tools/canary-ctl/test/helm.test.ts`

- [ ] **Step 1: Write the failing test**

`tools/canary-ctl/test/helm.test.ts`:

```typescript
import { describe, expect, it, vi } from "vitest";

vi.mock("../src/exec.js", () => ({
  run: vi.fn(async () => ({ stdout: "[]", stderr: "" })),
}));

import { run } from "../src/exec.js";
import { upgradeInstallCanary, uninstallCanary, listReleases } from "../src/helm.js";

const runMock = vi.mocked(run);

describe("helm shell-outs", () => {
  it("upgradeInstallCanary passes both values files and the image tag", async () => {
    runMock.mockResolvedValue({ stdout: "", stderr: "" });
    await upgradeInstallCanary({
      releaseName: "payment-service-canary",
      namespace: "services",
      chartPath: "deploy/helm/service-chart",
      valuesFile: "deploy/helm/values/payment-service.yaml",
      canaryOverlay: "deploy/helm/values/canary-overlay.yaml",
      tag: "v2",
      timeoutSeconds: 120,
    });
    expect(runMock).toHaveBeenCalledWith(
      "helm",
      [
        "upgrade", "--install", "payment-service-canary",
        "deploy/helm/service-chart",
        "-n", "services",
        "-f", "deploy/helm/values/payment-service.yaml",
        "-f", "deploy/helm/values/canary-overlay.yaml",
        "--set", "image.tag=v2",
        "--wait",
        "--timeout", "120s",
      ],
      expect.any(Object),
    );
  });

  it("uninstallCanary runs helm uninstall --wait", async () => {
    runMock.mockResolvedValue({ stdout: "", stderr: "" });
    await uninstallCanary("order-service-canary", "services");
    expect(runMock).toHaveBeenCalledWith(
      "helm",
      ["uninstall", "order-service-canary", "-n", "services", "--wait"],
      expect.any(Object),
    );
  });

  it("listReleases returns parsed JSON array", async () => {
    runMock.mockResolvedValue({
      stdout: JSON.stringify([
        { name: "payment-service", status: "deployed" },
        { name: "payment-service-canary", status: "deployed" },
      ]),
      stderr: "",
    });
    const releases = await listReleases("services");
    expect(releases.map((r) => r.name)).toEqual([
      "payment-service",
      "payment-service-canary",
    ]);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
pnpm --filter @canary/canary-ctl test
```

Expected: FAIL — `helm.js` does not exist.

- [ ] **Step 3: Write the implementation**

`tools/canary-ctl/src/helm.ts`:

```typescript
import { run } from "./exec.js";

export interface UpgradeInstallArgs {
  releaseName: string;
  namespace: string;
  chartPath: string;
  valuesFile: string;
  canaryOverlay: string;
  tag: string;
  timeoutSeconds: number;
}

export async function upgradeInstallCanary(args: UpgradeInstallArgs): Promise<void> {
  await run(
    "helm",
    [
      "upgrade", "--install", args.releaseName,
      args.chartPath,
      "-n", args.namespace,
      "-f", args.valuesFile,
      "-f", args.canaryOverlay,
      "--set", `image.tag=${args.tag}`,
      "--wait",
      "--timeout", `${args.timeoutSeconds}s`,
    ],
    { timeoutMs: (args.timeoutSeconds + 30) * 1000 },
  );
}

export async function uninstallCanary(releaseName: string, namespace: string): Promise<void> {
  await run(
    "helm",
    ["uninstall", releaseName, "-n", namespace, "--wait"],
    { timeoutMs: 120_000, ignoreError: true },
  );
}

export interface HelmRelease {
  name: string;
  status: string;
}

export async function listReleases(namespace: string): Promise<HelmRelease[]> {
  const r = await run("helm", ["list", "-n", namespace, "-o", "json"], {});
  return JSON.parse(r.stdout) as HelmRelease[];
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
pnpm --filter @canary/canary-ctl test
```

Expected: all helm tests PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/canary-ctl/src/helm.ts tools/canary-ctl/test/helm.test.ts
git commit -m "feat(canary-ctl): helm shell-out helpers"
```

---

## Task 7: `deploy-canary` command

**Files:**
- Create: `tools/canary-ctl/src/commands/deploy-canary.ts`
- Create: `tools/canary-ctl/test/deploy-canary.test.ts`

- [ ] **Step 1: Write the failing test**

`tools/canary-ctl/test/deploy-canary.test.ts`:

```typescript
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

vi.mock("../src/helm.js", () => ({
  upgradeInstallCanary: vi.fn(async () => {}),
  uninstallCanary: vi.fn(async () => {}),
  listReleases: vi.fn(async () => []),
}));
vi.mock("../src/kubectl.js", () => ({
  patchVirtualService: vi.fn(async () => {}),
  rolloutStatus: vi.fn(async () => {}),
  buildHeaderRulePatch: (svc: string) => `HEADER:${svc}`,
  buildDefaultOnlyPatch: (svc: string) => `DEFAULT:${svc}`,
  getDeploymentReady: vi.fn(),
  getVirtualServiceRules: vi.fn(),
}));

import { upgradeInstallCanary, uninstallCanary } from "../src/helm.js";
import { patchVirtualService } from "../src/kubectl.js";
import { readState, deleteState, type CanaryState } from "../src/state.js";
import { deployCanary } from "../src/commands/deploy-canary.js";

const upgradeMock = vi.mocked(upgradeInstallCanary);
const uninstallMock = vi.mocked(uninstallCanary);
const patchMock = vi.mocked(patchVirtualService);

describe("deploy-canary", () => {
  let dir: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "deploy-canary-"));
    upgradeMock.mockReset().mockResolvedValue(undefined);
    uninstallMock.mockReset().mockResolvedValue(undefined);
    patchMock.mockReset().mockResolvedValue(undefined);
  });

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  it("happy path: upgrade, then patch VS, then state phase=active", async () => {
    await deployCanary({ service: "payment-service", tag: "v2", stateDir: dir, repoRoot: "/repo" });

    expect(upgradeMock).toHaveBeenCalledOnce();
    expect(patchMock).toHaveBeenCalledWith("payment-service", "services", "HEADER:payment-service");
    const state = readState(dir, "payment-service") as CanaryState;
    expect(state.phase).toBe("active");
    expect(state.tag).toBe("v2");
    expect(state.deployedAt).toMatch(/^\d{4}-\d{2}-\d{2}T/);
  });

  it("helm failure auto-rolls back and rethrows", async () => {
    upgradeMock.mockRejectedValueOnce(new Error("rollout deadline exceeded"));
    await expect(
      deployCanary({ service: "payment-service", tag: "nope", stateDir: dir, repoRoot: "/repo" }),
    ).rejects.toThrow(/rollout deadline/);

    // Auto-rollback fired:
    expect(patchMock).toHaveBeenCalledWith("payment-service", "services", "DEFAULT:payment-service");
    expect(uninstallMock).toHaveBeenCalledWith("payment-service-canary", "services");
    expect(readState(dir, "payment-service")).toBeNull();
  });

  it("patch failure after successful helm install auto-rolls back", async () => {
    patchMock.mockRejectedValueOnce(new Error("kubectl patch error"));
    await expect(
      deployCanary({ service: "order-service", tag: "v2", stateDir: dir, repoRoot: "/repo" }),
    ).rejects.toThrow(/kubectl patch error/);

    // Auto-rollback uninstalls the release.
    expect(uninstallMock).toHaveBeenCalledWith("order-service-canary", "services");
    expect(readState(dir, "order-service")).toBeNull();
  });

  it("unknown service throws before any side effects", async () => {
    await expect(
      deployCanary({ service: "nope", tag: "v2", stateDir: dir, repoRoot: "/repo" }),
    ).rejects.toThrow(/unknown service: nope/i);
    expect(upgradeMock).not.toHaveBeenCalled();
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
pnpm --filter @canary/canary-ctl test
```

Expected: FAIL — `commands/deploy-canary.js` does not exist.

- [ ] **Step 3: Write the implementation**

`tools/canary-ctl/src/commands/deploy-canary.ts`:

```typescript
import { resolve } from "node:path";
import { lookup } from "../registry.js";
import { upgradeInstallCanary, uninstallCanary } from "../helm.js";
import {
  patchVirtualService,
  buildHeaderRulePatch,
  buildDefaultOnlyPatch,
} from "../kubectl.js";
import { writeState, deleteState } from "../state.js";
import { log } from "../logger.js";

export interface DeployCanaryOpts {
  service: string;
  tag: string;
  stateDir: string;
  repoRoot: string;
  timeoutSeconds?: number;
}

export async function deployCanary(opts: DeployCanaryOpts): Promise<void> {
  const entry = lookup(opts.service);
  const timeout = opts.timeoutSeconds ?? 120;

  // Phase: deploying
  writeState(opts.stateDir, {
    service: entry.name,
    phase: "deploying",
    tag: opts.tag,
    deployedAt: new Date().toISOString(),
  });
  log("info", "phase=deploying", { service: entry.name, tag: opts.tag });

  try {
    await upgradeInstallCanary({
      releaseName: entry.helmReleaseCanary,
      namespace: entry.namespace,
      chartPath: resolve(opts.repoRoot, entry.chartPath),
      valuesFile: resolve(opts.repoRoot, entry.valuesFile),
      canaryOverlay: resolve(opts.repoRoot, entry.canaryOverlay),
      tag: opts.tag,
      timeoutSeconds: timeout,
    });
  } catch (err) {
    await autoRollback(opts, entry.name, entry.helmReleaseCanary, entry.virtualService, entry.namespace);
    throw err;
  }

  // Phase: deployment-ready
  writeState(opts.stateDir, {
    service: entry.name,
    phase: "deployment-ready",
    tag: opts.tag,
    deployedAt: new Date().toISOString(),
  });
  log("info", "phase=deployment-ready", { service: entry.name });

  // Apply header rule.
  try {
    await patchVirtualService(entry.virtualService, entry.namespace, buildHeaderRulePatch(entry.virtualService));
  } catch (err) {
    await autoRollback(opts, entry.name, entry.helmReleaseCanary, entry.virtualService, entry.namespace);
    throw err;
  }

  // Phase: active
  writeState(opts.stateDir, {
    service: entry.name,
    phase: "active",
    tag: opts.tag,
    deployedAt: new Date().toISOString(),
  });
  log("info", "phase=active", { service: entry.name, tag: opts.tag });
}

async function autoRollback(
  opts: DeployCanaryOpts,
  service: string,
  releaseName: string,
  vsName: string,
  namespace: string,
): Promise<void> {
  log("warn", "auto-rollback", { service });
  writeState(opts.stateDir, {
    service,
    phase: "rolling-back",
    tag: opts.tag,
    deployedAt: new Date().toISOString(),
  });
  // Header rule first (idempotent — no-op if it was never applied).
  try {
    await patchVirtualService(vsName, namespace, buildDefaultOnlyPatch(vsName));
  } catch (e) {
    log("warn", "auto-rollback: VS patch failed (continuing)", { error: (e as Error).message });
  }
  // Helm uninstall (ignoreError=true inside helm.uninstallCanary).
  await uninstallCanary(releaseName, namespace);
  deleteState(opts.stateDir, service);
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
pnpm --filter @canary/canary-ctl test
```

Expected: all deploy-canary tests PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/canary-ctl/src/commands/deploy-canary.ts tools/canary-ctl/test/deploy-canary.test.ts
git commit -m "feat(canary-ctl): deploy-canary command with auto-rollback on failure"
```

---

## Task 8: `rollback` command

**Files:**
- Create: `tools/canary-ctl/src/commands/rollback.ts`
- Create: `tools/canary-ctl/test/rollback.test.ts`

- [ ] **Step 1: Write the failing test**

`tools/canary-ctl/test/rollback.test.ts`:

```typescript
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

vi.mock("../src/helm.js", () => ({
  uninstallCanary: vi.fn(async () => {}),
  listReleases: vi.fn(async () => [] as Array<{ name: string; status: string }>),
}));
vi.mock("../src/kubectl.js", () => ({
  patchVirtualService: vi.fn(async () => {}),
  buildDefaultOnlyPatch: (svc: string) => `DEFAULT:${svc}`,
  getVirtualServiceRules: vi.fn(),
}));

import { uninstallCanary, listReleases } from "../src/helm.js";
import { patchVirtualService, getVirtualServiceRules } from "../src/kubectl.js";
import { writeState, readState } from "../src/state.js";
import { rollback } from "../src/commands/rollback.js";

const uninstallMock = vi.mocked(uninstallCanary);
const patchMock = vi.mocked(patchVirtualService);
const listMock = vi.mocked(listReleases);
const getVsMock = vi.mocked(getVirtualServiceRules);

describe("rollback", () => {
  let dir: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "rollback-"));
    uninstallMock.mockReset().mockResolvedValue(undefined);
    patchMock.mockReset().mockResolvedValue(undefined);
    listMock.mockReset().mockResolvedValue([]);
    getVsMock.mockReset().mockResolvedValue({ hasHeaderRule: false, ruleNames: ["default"] });
  });

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  it("active state: header rule first, then helm uninstall, then state cleared", async () => {
    writeState(dir, {
      service: "payment-service",
      phase: "active",
      tag: "v2",
      deployedAt: "2026-05-09T18:30:00Z",
    });
    listMock.mockResolvedValue([{ name: "payment-service-canary", status: "deployed" }]);

    await rollback({ service: "payment-service", stateDir: dir, graceSeconds: 0 });

    // Order: patch was called BEFORE uninstall.
    expect(patchMock).toHaveBeenCalledWith("payment-service", "services", "DEFAULT:payment-service");
    expect(uninstallMock).toHaveBeenCalledWith("payment-service-canary", "services");
    expect(patchMock.mock.invocationCallOrder[0]).toBeLessThan(uninstallMock.mock.invocationCallOrder[0]);
    expect(readState(dir, "payment-service")).toBeNull();
  });

  it("absent state with no canary release: no-op, exits clean", async () => {
    listMock.mockResolvedValue([{ name: "payment-service", status: "deployed" }]); // only stable
    await rollback({ service: "payment-service", stateDir: dir, graceSeconds: 0 });
    expect(uninstallMock).not.toHaveBeenCalled();
    expect(patchMock).not.toHaveBeenCalled();
  });

  it("absent state but orphan release: uninstall it", async () => {
    listMock.mockResolvedValue([
      { name: "payment-service", status: "deployed" },
      { name: "payment-service-canary", status: "deployed" },
    ]);
    await rollback({ service: "payment-service", stateDir: dir, graceSeconds: 0 });
    expect(uninstallMock).toHaveBeenCalledWith("payment-service-canary", "services");
  });

  it("active state but VS already lacks header rule: still uninstall, still idempotent", async () => {
    writeState(dir, {
      service: "order-service",
      phase: "active",
      tag: "v1",
      deployedAt: "2026-05-09T18:30:00Z",
    });
    listMock.mockResolvedValue([{ name: "order-service-canary", status: "deployed" }]);
    getVsMock.mockResolvedValue({ hasHeaderRule: false, ruleNames: ["default"] });
    await rollback({ service: "order-service", stateDir: dir, graceSeconds: 0 });
    expect(uninstallMock).toHaveBeenCalled();
    expect(readState(dir, "order-service")).toBeNull();
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
pnpm --filter @canary/canary-ctl test
```

Expected: FAIL — `commands/rollback.js` does not exist.

- [ ] **Step 3: Write the implementation**

`tools/canary-ctl/src/commands/rollback.ts`:

```typescript
import { setTimeout as sleep } from "node:timers/promises";
import { lookup } from "../registry.js";
import { uninstallCanary, listReleases } from "../helm.js";
import { patchVirtualService, buildDefaultOnlyPatch } from "../kubectl.js";
import { readState, deleteState, writeState } from "../state.js";
import { log } from "../logger.js";

export interface RollbackOpts {
  service: string;
  stateDir: string;
  graceSeconds: number;
}

export async function rollback(opts: RollbackOpts): Promise<void> {
  const entry = lookup(opts.service);
  const state = readState(opts.stateDir, entry.name);
  const releases = await listReleases(entry.namespace);
  const canaryReleasePresent = releases.some((r) => r.name === entry.helmReleaseCanary);

  if (!state && !canaryReleasePresent) {
    log("info", "rollback: nothing to do", { service: entry.name });
    return;
  }

  // Phase: rolling-back (only if state existed)
  if (state) {
    writeState(opts.stateDir, { ...state, phase: "rolling-back" });
  }

  // 1. Header rule off (idempotent).
  await patchVirtualService(
    entry.virtualService,
    entry.namespace,
    buildDefaultOnlyPatch(entry.virtualService),
  );
  log("info", "rollback: VS reverted to default-only", { service: entry.name });

  // 2. Grace.
  if (opts.graceSeconds > 0) {
    log("info", "rollback: grace sleep", { seconds: opts.graceSeconds });
    await sleep(opts.graceSeconds * 1000);
  }

  // 3. Uninstall canary release (idempotent if absent — helm uninstall ignoreError).
  if (canaryReleasePresent) {
    await uninstallCanary(entry.helmReleaseCanary, entry.namespace);
    log("info", "rollback: canary release uninstalled", { release: entry.helmReleaseCanary });
  }

  // 4. Clear state.
  deleteState(opts.stateDir, entry.name);
  log("info", "rollback: state cleared", { service: entry.name });
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
pnpm --filter @canary/canary-ctl test
```

Expected: all rollback tests PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/canary-ctl/src/commands/rollback.ts tools/canary-ctl/test/rollback.test.ts
git commit -m "feat(canary-ctl): rollback command (header rule first, grace, uninstall, clear state)"
```

---

## Task 9: `status` command

**Files:**
- Create: `tools/canary-ctl/src/commands/status.ts`
- Create: `tools/canary-ctl/test/status.test.ts`

- [ ] **Step 1: Write the failing test**

`tools/canary-ctl/test/status.test.ts`:

```typescript
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

vi.mock("../src/helm.js", () => ({ listReleases: vi.fn() }));
vi.mock("../src/kubectl.js", () => ({
  getDeploymentReady: vi.fn(),
  getVirtualServiceRules: vi.fn(),
}));

import { listReleases } from "../src/helm.js";
import { getDeploymentReady, getVirtualServiceRules } from "../src/kubectl.js";
import { writeState } from "../src/state.js";
import { computeStatus } from "../src/commands/status.js";

const listMock = vi.mocked(listReleases);
const deployMock = vi.mocked(getDeploymentReady);
const vsMock = vi.mocked(getVirtualServiceRules);

describe("status", () => {
  let dir: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "status-"));
    listMock.mockReset();
    deployMock.mockReset();
    vsMock.mockReset();
  });

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  it("clean cluster, no state: drift=none, canary=absent", async () => {
    listMock.mockResolvedValue([{ name: "payment-service", status: "deployed" }]);
    deployMock.mockResolvedValue({ ready: 0, total: 0, exists: false });
    vsMock.mockResolvedValue({ hasHeaderRule: false, ruleNames: ["default"] });
    const s = await computeStatus({ service: "payment-service", stateDir: dir });
    expect(s.statePhase).toBeNull();
    expect(s.helmCanaryPresent).toBe(false);
    expect(s.vsHasHeaderRule).toBe(false);
    expect(s.drift).toEqual([]);
  });

  it("active state with all cluster pieces present: drift=none", async () => {
    writeState(dir, { service: "payment-service", phase: "active", tag: "v2", deployedAt: "2026-05-09T18:30:00Z" });
    listMock.mockResolvedValue([
      { name: "payment-service", status: "deployed" },
      { name: "payment-service-canary", status: "deployed" },
    ]);
    deployMock.mockResolvedValue({ ready: 1, total: 1, exists: true });
    vsMock.mockResolvedValue({ hasHeaderRule: true, ruleNames: ["canary-by-header", "default"] });
    const s = await computeStatus({ service: "payment-service", stateDir: dir });
    expect(s.statePhase).toBe("active");
    expect(s.drift).toEqual([]);
  });

  it("state says active but VS lacks header rule: drift detected", async () => {
    writeState(dir, { service: "order-service", phase: "active", tag: "v2", deployedAt: "2026-05-09T18:30:00Z" });
    listMock.mockResolvedValue([
      { name: "order-service", status: "deployed" },
      { name: "order-service-canary", status: "deployed" },
    ]);
    deployMock.mockResolvedValue({ ready: 1, total: 1, exists: true });
    vsMock.mockResolvedValue({ hasHeaderRule: false, ruleNames: ["default"] });
    const s = await computeStatus({ service: "order-service", stateDir: dir });
    expect(s.drift).toContain("state phase=active but VS has no header rule");
  });

  it("no state but canary release present: drift detected", async () => {
    listMock.mockResolvedValue([
      { name: "audit-service", status: "deployed" },
      { name: "audit-service-canary", status: "deployed" },
    ]);
    deployMock.mockResolvedValue({ ready: 1, total: 1, exists: true });
    vsMock.mockResolvedValue({ hasHeaderRule: false, ruleNames: ["default"] });
    const s = await computeStatus({ service: "audit-service", stateDir: dir });
    expect(s.drift).toContain("state file missing but canary release present");
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
pnpm --filter @canary/canary-ctl test
```

Expected: FAIL.

- [ ] **Step 3: Write the implementation**

`tools/canary-ctl/src/commands/status.ts`:

```typescript
import { lookup } from "../registry.js";
import { listReleases } from "../helm.js";
import { getDeploymentReady, getVirtualServiceRules } from "../kubectl.js";
import { readState, type CanaryPhase } from "../state.js";

export interface StatusOpts {
  service: string;
  stateDir: string;
}

export interface CanaryStatus {
  service: string;
  statePhase: CanaryPhase | null;
  stateTag: string | null;
  stateDeployedAt: string | null;
  helmCanaryPresent: boolean;
  helmCanaryStatus: string | null;
  deploymentReady: number;
  deploymentTotal: number;
  deploymentExists: boolean;
  vsHasHeaderRule: boolean;
  vsRuleNames: string[];
  drift: string[];
}

export async function computeStatus(opts: StatusOpts): Promise<CanaryStatus> {
  const entry = lookup(opts.service);
  const state = readState(opts.stateDir, entry.name);
  const releases = await listReleases(entry.namespace);
  const canary = releases.find((r) => r.name === entry.helmReleaseCanary);
  const dep = await getDeploymentReady(entry.helmReleaseCanary, entry.namespace);
  const vs = await getVirtualServiceRules(entry.virtualService, entry.namespace);

  const drift: string[] = [];
  if (state && state.phase === "active" && !vs.hasHeaderRule) {
    drift.push("state phase=active but VS has no header rule");
  }
  if (state && state.phase === "active" && !canary) {
    drift.push("state phase=active but canary release missing");
  }
  if (!state && canary) {
    drift.push("state file missing but canary release present");
  }
  if (!state && vs.hasHeaderRule) {
    drift.push("state file missing but VS still has header rule");
  }

  return {
    service: entry.name,
    statePhase: state?.phase ?? null,
    stateTag: state?.tag ?? null,
    stateDeployedAt: state?.deployedAt ?? null,
    helmCanaryPresent: Boolean(canary),
    helmCanaryStatus: canary?.status ?? null,
    deploymentReady: dep.ready,
    deploymentTotal: dep.total,
    deploymentExists: dep.exists,
    vsHasHeaderRule: vs.hasHeaderRule,
    vsRuleNames: vs.ruleNames,
    drift,
  };
}

export function formatStatusText(s: CanaryStatus): string {
  const lines: string[] = [];
  lines.push(`${s.service}:`);
  lines.push(`  state file: ${s.statePhase ?? "absent"}${s.statePhase ? ` (tag ${s.stateTag}, deployed ${s.stateDeployedAt})` : ""}`);
  lines.push(`  helm release ${s.service}-canary: ${s.helmCanaryPresent ? `present, status ${s.helmCanaryStatus}` : "absent"}`);
  if (s.deploymentExists) {
    lines.push(`  deployment ready: ${s.deploymentReady}/${s.deploymentTotal}`);
  }
  lines.push(`  virtualservice header rule: ${s.vsHasHeaderRule ? "present" : "absent"} (rules: ${s.vsRuleNames.join(", ") || "none"})`);
  lines.push(`  drift: ${s.drift.length === 0 ? "none" : ""}`);
  for (const d of s.drift) lines.push(`    - ${d}`);
  return lines.join("\n");
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
pnpm --filter @canary/canary-ctl test
```

Expected: all status tests PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/canary-ctl/src/commands/status.ts tools/canary-ctl/test/status.test.ts
git commit -m "feat(canary-ctl): status command with drift detection (text + JSON)"
```

---

## Task 10: `reconcile` command

**Files:**
- Create: `tools/canary-ctl/src/commands/reconcile.ts`
- Create: `tools/canary-ctl/test/reconcile.test.ts`

- [ ] **Step 1: Write the failing test**

`tools/canary-ctl/test/reconcile.test.ts`:

```typescript
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

vi.mock("../src/helm.js", () => ({
  listReleases: vi.fn(),
  uninstallCanary: vi.fn(async () => {}),
}));
vi.mock("../src/kubectl.js", () => ({
  patchVirtualService: vi.fn(async () => {}),
  buildHeaderRulePatch: (svc: string) => `HEADER:${svc}`,
  buildDefaultOnlyPatch: (svc: string) => `DEFAULT:${svc}`,
  getDeploymentReady: vi.fn(),
  getVirtualServiceRules: vi.fn(),
}));

import { listReleases, uninstallCanary } from "../src/helm.js";
import { patchVirtualService, getDeploymentReady, getVirtualServiceRules } from "../src/kubectl.js";
import { writeState, readState } from "../src/state.js";
import { reconcile } from "../src/commands/reconcile.js";

const listMock = vi.mocked(listReleases);
const uninstallMock = vi.mocked(uninstallCanary);
const patchMock = vi.mocked(patchVirtualService);
const depMock = vi.mocked(getDeploymentReady);
const vsMock = vi.mocked(getVirtualServiceRules);

describe("reconcile", () => {
  let dir: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "reconcile-"));
    listMock.mockReset();
    uninstallMock.mockReset().mockResolvedValue(undefined);
    patchMock.mockReset().mockResolvedValue(undefined);
    depMock.mockReset();
    vsMock.mockReset();
  });

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  it("clean state + clean cluster: no-op", async () => {
    listMock.mockResolvedValue([]);
    depMock.mockResolvedValue({ ready: 0, total: 0, exists: false });
    vsMock.mockResolvedValue({ hasHeaderRule: false, ruleNames: ["default"] });
    const r = await reconcile({ service: "payment-service", stateDir: dir, adopt: false });
    expect(r.action).toBe("no-op");
    expect(uninstallMock).not.toHaveBeenCalled();
    expect(patchMock).not.toHaveBeenCalled();
  });

  it("orphan release with no state: uninstalls (default behavior)", async () => {
    listMock.mockResolvedValue([{ name: "payment-service-canary", status: "deployed" }]);
    depMock.mockResolvedValue({ ready: 1, total: 1, exists: true });
    vsMock.mockResolvedValue({ hasHeaderRule: false, ruleNames: ["default"] });
    const r = await reconcile({ service: "payment-service", stateDir: dir, adopt: false });
    expect(r.action).toBe("rollback-orphan");
    expect(uninstallMock).toHaveBeenCalledWith("payment-service-canary", "services");
  });

  it("orphan release with no state and --adopt: writes state + applies header rule", async () => {
    listMock.mockResolvedValue([{ name: "payment-service-canary", status: "deployed" }]);
    depMock.mockResolvedValue({ ready: 1, total: 1, exists: true });
    vsMock.mockResolvedValue({ hasHeaderRule: false, ruleNames: ["default"] });
    const r = await reconcile({ service: "payment-service", stateDir: dir, adopt: true });
    expect(r.action).toBe("adopt");
    expect(patchMock).toHaveBeenCalledWith("payment-service", "services", "HEADER:payment-service");
    expect(readState(dir, "payment-service")?.phase).toBe("active");
  });

  it("phase=deployment-ready + release Ready + no header rule: applies header rule, sets active", async () => {
    writeState(dir, { service: "order-service", phase: "deployment-ready", tag: "v2", deployedAt: "2026-05-09T18:30:00Z" });
    listMock.mockResolvedValue([{ name: "order-service-canary", status: "deployed" }]);
    depMock.mockResolvedValue({ ready: 1, total: 1, exists: true });
    vsMock.mockResolvedValue({ hasHeaderRule: false, ruleNames: ["default"] });
    const r = await reconcile({ service: "order-service", stateDir: dir, adopt: false });
    expect(r.action).toBe("complete-deploy");
    expect(patchMock).toHaveBeenCalledWith("order-service", "services", "HEADER:order-service");
    expect(readState(dir, "order-service")?.phase).toBe("active");
  });

  it("phase=active + release missing: removes header rule + clears state", async () => {
    writeState(dir, { service: "audit-service", phase: "active", tag: "v1", deployedAt: "2026-05-09T18:30:00Z" });
    listMock.mockResolvedValue([]);
    depMock.mockResolvedValue({ ready: 0, total: 0, exists: false });
    vsMock.mockResolvedValue({ hasHeaderRule: true, ruleNames: ["canary-by-header", "default"] });
    const r = await reconcile({ service: "audit-service", stateDir: dir, adopt: false });
    expect(r.action).toBe("rollback-stale-state");
    expect(patchMock).toHaveBeenCalledWith("audit-service", "services", "DEFAULT:audit-service");
    expect(readState(dir, "audit-service")).toBeNull();
  });

  it("phase=rolling-back: finishes rollback", async () => {
    writeState(dir, { service: "inventory-service", phase: "rolling-back", tag: "v1", deployedAt: "2026-05-09T18:30:00Z" });
    listMock.mockResolvedValue([{ name: "inventory-service-canary", status: "deployed" }]);
    depMock.mockResolvedValue({ ready: 1, total: 1, exists: true });
    vsMock.mockResolvedValue({ hasHeaderRule: true, ruleNames: ["canary-by-header", "default"] });
    const r = await reconcile({ service: "inventory-service", stateDir: dir, adopt: false });
    expect(r.action).toBe("finish-rollback");
    expect(patchMock).toHaveBeenCalledWith("inventory-service", "services", "DEFAULT:inventory-service");
    expect(uninstallMock).toHaveBeenCalled();
    expect(readState(dir, "inventory-service")).toBeNull();
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
pnpm --filter @canary/canary-ctl test
```

Expected: FAIL.

- [ ] **Step 3: Write the implementation**

`tools/canary-ctl/src/commands/reconcile.ts`:

```typescript
import { lookup } from "../registry.js";
import { listReleases, uninstallCanary } from "../helm.js";
import {
  patchVirtualService,
  buildHeaderRulePatch,
  buildDefaultOnlyPatch,
  getDeploymentReady,
  getVirtualServiceRules,
} from "../kubectl.js";
import { readState, writeState, deleteState } from "../state.js";
import { log } from "../logger.js";

export type ReconcileAction =
  | "no-op"
  | "complete-deploy"
  | "finish-rollback"
  | "rollback-orphan"
  | "rollback-stale-state"
  | "adopt"
  | "clear-stale-state"
  | "remove-orphan-vs-rule"
  | "drift-fix";

export interface ReconcileOpts {
  service: string;
  stateDir: string;
  adopt: boolean;
}

export interface ReconcileResult {
  action: ReconcileAction;
  notes: string[];
}

export async function reconcile(opts: ReconcileOpts): Promise<ReconcileResult> {
  const entry = lookup(opts.service);
  const state = readState(opts.stateDir, entry.name);
  const releases = await listReleases(entry.namespace);
  const canaryReleasePresent = releases.some((r) => r.name === entry.helmReleaseCanary);
  const dep = await getDeploymentReady(entry.helmReleaseCanary, entry.namespace);
  const vs = await getVirtualServiceRules(entry.virtualService, entry.namespace);
  const notes: string[] = [];
  let action: ReconcileAction = "no-op";

  // No state file
  if (!state) {
    if (!canaryReleasePresent && !vs.hasHeaderRule) {
      action = "no-op";
    } else if (!canaryReleasePresent && vs.hasHeaderRule) {
      // Orphan VS rule: remove it.
      await patchVirtualService(entry.virtualService, entry.namespace, buildDefaultOnlyPatch(entry.virtualService));
      action = "remove-orphan-vs-rule";
    } else if (canaryReleasePresent && opts.adopt) {
      // Adopt the orphan release.
      writeState(opts.stateDir, {
        service: entry.name,
        phase: "active",
        tag: "adopted",
        deployedAt: new Date().toISOString(),
      });
      if (!vs.hasHeaderRule) {
        await patchVirtualService(entry.virtualService, entry.namespace, buildHeaderRulePatch(entry.virtualService));
      }
      action = "adopt";
    } else {
      // canaryReleasePresent and !adopt → roll back.
      if (vs.hasHeaderRule) {
        await patchVirtualService(entry.virtualService, entry.namespace, buildDefaultOnlyPatch(entry.virtualService));
      }
      await uninstallCanary(entry.helmReleaseCanary, entry.namespace);
      action = "rollback-orphan";
    }
    log("info", `reconcile: ${action}`, { service: entry.name });
    return { action, notes };
  }

  // State file present.
  switch (state.phase) {
    case "deploying":
      if (!canaryReleasePresent) {
        deleteState(opts.stateDir, entry.name);
        action = "clear-stale-state";
      } else if (dep.exists && dep.ready === dep.total && dep.total > 0) {
        // Progress through deployment-ready → active.
        await patchVirtualService(entry.virtualService, entry.namespace, buildHeaderRulePatch(entry.virtualService));
        writeState(opts.stateDir, { ...state, phase: "active" });
        action = "complete-deploy";
      } else {
        // Release exists but not Ready — roll back.
        if (vs.hasHeaderRule) {
          await patchVirtualService(entry.virtualService, entry.namespace, buildDefaultOnlyPatch(entry.virtualService));
        }
        await uninstallCanary(entry.helmReleaseCanary, entry.namespace);
        deleteState(opts.stateDir, entry.name);
        action = "rollback-stale-state";
      }
      break;

    case "deployment-ready":
      if (!canaryReleasePresent) {
        if (vs.hasHeaderRule) {
          await patchVirtualService(entry.virtualService, entry.namespace, buildDefaultOnlyPatch(entry.virtualService));
        }
        deleteState(opts.stateDir, entry.name);
        action = "rollback-stale-state";
      } else if (!vs.hasHeaderRule) {
        await patchVirtualService(entry.virtualService, entry.namespace, buildHeaderRulePatch(entry.virtualService));
        writeState(opts.stateDir, { ...state, phase: "active" });
        action = "complete-deploy";
      } else {
        writeState(opts.stateDir, { ...state, phase: "active" });
        action = "drift-fix";
      }
      break;

    case "active":
      if (!canaryReleasePresent) {
        if (vs.hasHeaderRule) {
          await patchVirtualService(entry.virtualService, entry.namespace, buildDefaultOnlyPatch(entry.virtualService));
        }
        deleteState(opts.stateDir, entry.name);
        action = "rollback-stale-state";
      } else if (!vs.hasHeaderRule) {
        await patchVirtualService(entry.virtualService, entry.namespace, buildHeaderRulePatch(entry.virtualService));
        action = "drift-fix";
      } else {
        action = "no-op";
      }
      break;

    case "rolling-back":
      if (vs.hasHeaderRule) {
        await patchVirtualService(entry.virtualService, entry.namespace, buildDefaultOnlyPatch(entry.virtualService));
      }
      if (canaryReleasePresent) {
        await uninstallCanary(entry.helmReleaseCanary, entry.namespace);
      }
      deleteState(opts.stateDir, entry.name);
      action = "finish-rollback";
      break;
  }

  log("info", `reconcile: ${action}`, { service: entry.name, phase: state.phase });
  return { action, notes };
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
pnpm --filter @canary/canary-ctl test
```

Expected: all reconcile tests PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/canary-ctl/src/commands/reconcile.ts tools/canary-ctl/test/reconcile.test.ts
git commit -m "feat(canary-ctl): reconcile command with full state×cluster decision table"
```

---

## Task 11: CLI entrypoint with commander

**Files:**
- Modify: `tools/canary-ctl/src/index.ts`

- [ ] **Step 1: Replace placeholder `index.ts` with commander wiring**

`tools/canary-ctl/src/index.ts`:

```typescript
import { Command } from "commander";
import { homedir } from "node:os";
import { join } from "node:path";
import { setVerbose } from "./logger.js";
import { deployCanary } from "./commands/deploy-canary.js";
import { rollback } from "./commands/rollback.js";
import { computeStatus, formatStatusText } from "./commands/status.js";
import { reconcile } from "./commands/reconcile.js";

interface GlobalOpts {
  stateDir: string;
  repoRoot: string;
  graceSeconds: number;
  verbose: boolean;
}

function makeProgram(): Command {
  const program = new Command();
  program
    .name("canary-ctl")
    .description("Manage per-service canary lifecycle (Helm release + VirtualService header rule + state file)")
    .option("--state-dir <path>", "directory for per-service state files", join(homedir(), ".canary-ctl"))
    .option("--repo-root <path>", "repo root for resolving Helm chart and values paths", process.cwd())
    .option("--grace-seconds <n>", "rollback grace period (sec)", (v) => parseInt(v, 10), 10)
    .option("--verbose", "enable debug logging", false);

  program.command("deploy-canary <service> <tag>")
    .description("Create canary release and apply VS header-match rule")
    .action(async (service: string, tag: string) => {
      const o = program.opts<GlobalOpts>();
      setVerbose(o.verbose);
      await deployCanary({ service, tag, stateDir: o.stateDir, repoRoot: o.repoRoot });
    });

  program.command("rollback <service>")
    .description("Remove header rule, drain, uninstall canary release, clear state")
    .action(async (service: string) => {
      const o = program.opts<GlobalOpts>();
      setVerbose(o.verbose);
      await rollback({ service, stateDir: o.stateDir, graceSeconds: o.graceSeconds });
    });

  program.command("status <service>")
    .description("Print canary state for a service (text or --json)")
    .option("--json", "machine-readable output", false)
    .action(async (service: string, cmdOpts: { json: boolean }) => {
      const o = program.opts<GlobalOpts>();
      setVerbose(o.verbose);
      const s = await computeStatus({ service, stateDir: o.stateDir });
      if (cmdOpts.json) {
        process.stdout.write(JSON.stringify(s, null, 2) + "\n");
      } else {
        process.stdout.write(formatStatusText(s) + "\n");
      }
      if (s.drift.length > 0) process.exit(2);
    });

  program.command("reconcile <service>")
    .description("Inspect cluster + state and bring them to a consistent state")
    .option("--adopt", "adopt orphan canary releases instead of rolling back", false)
    .action(async (service: string, cmdOpts: { adopt: boolean }) => {
      const o = program.opts<GlobalOpts>();
      setVerbose(o.verbose);
      const r = await reconcile({ service, stateDir: o.stateDir, adopt: cmdOpts.adopt });
      process.stdout.write(`reconcile: ${r.action}\n`);
    });

  return program;
}

export async function run(argv: string[]): Promise<void> {
  const program = makeProgram();
  try {
    await program.parseAsync(argv);
  } catch (err) {
    process.stderr.write(`canary-ctl: ${(err as Error).message}\n`);
    process.exit(1);
  }
}
```

- [ ] **Step 2: Build and check `--help`**

```bash
pnpm --filter @canary/canary-ctl build
node tools/canary-ctl/bin/canary-ctl --help
```

Expected: prints commander-rendered help with the four subcommands.

- [ ] **Step 3: Manually verify each subcommand `--help` works**

```bash
node tools/canary-ctl/bin/canary-ctl deploy-canary --help
node tools/canary-ctl/bin/canary-ctl rollback --help
node tools/canary-ctl/bin/canary-ctl status --help
node tools/canary-ctl/bin/canary-ctl reconcile --help
```

Expected: each prints its own help block.

- [ ] **Step 4: Commit**

```bash
git add tools/canary-ctl/src/index.ts
git commit -m "feat(canary-ctl): commander entrypoint wiring all 4 subcommands"
```

---

## Task 12: `traffic-cli` implementation

**Files:**
- Modify: `tools/traffic-cli/src/index.ts`
- Create: `tools/traffic-cli/test/index.test.ts`

- [ ] **Step 1: Write the failing test**

`tools/traffic-cli/test/index.test.ts`:

```typescript
import { describe, expect, it, vi } from "vitest";

vi.mock("axios", () => ({
  default: { post: vi.fn() },
}));

import axios from "axios";
import { sendOrder } from "../src/index.js";

const postMock = vi.mocked(axios.post);

describe("traffic-cli sendOrder", () => {
  it("default: no x-canary header", async () => {
    postMock.mockResolvedValue({ status: 201, data: { id: "ord-1" }, headers: {} });
    await sendOrder({ url: "http://localhost:8080", canary: false, user: "u1", sku: "sku-1", quantity: 1, amount: 100 });
    const [, , cfg] = postMock.mock.calls[0];
    expect((cfg?.headers as Record<string, string>)["x-canary"]).toBeUndefined();
  });

  it("--canary attaches the header", async () => {
    postMock.mockResolvedValue({ status: 201, data: { id: "ord-2" }, headers: {} });
    await sendOrder({ url: "http://localhost:8080", canary: true, user: "u1", sku: "sku-1", quantity: 1, amount: 100 });
    const [, , cfg] = postMock.mock.calls[0];
    expect((cfg?.headers as Record<string, string>)["x-canary"]).toBe("true");
  });

  it("returns response status, data, headers", async () => {
    postMock.mockResolvedValue({ status: 201, data: { id: "ord-3" }, headers: { server: "envoy" } });
    const r = await sendOrder({ url: "http://localhost:8080", canary: false, user: "u1", sku: "sku-1", quantity: 1, amount: 100 });
    expect(r.status).toBe(201);
    expect(r.data).toEqual({ id: "ord-3" });
    expect(r.headers).toEqual({ server: "envoy" });
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
pnpm --filter @canary/traffic-cli test
```

Expected: FAIL.

- [ ] **Step 3: Write the implementation**

`tools/traffic-cli/src/index.ts`:

```typescript
import { Command } from "commander";
import axios from "axios";

export interface SendOrderOpts {
  url: string;
  canary: boolean;
  user: string;
  sku: string;
  quantity: number;
  amount: number;
}

export interface SendResult {
  status: number;
  data: unknown;
  headers: Record<string, string>;
}

export async function sendOrder(opts: SendOrderOpts): Promise<SendResult> {
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (opts.canary) headers["x-canary"] = "true";
  const r = await axios.post(
    `${opts.url}/api/orders`,
    { userId: opts.user, sku: opts.sku, quantity: opts.quantity, amount: opts.amount },
    { headers, validateStatus: () => true },
  );
  return { status: r.status, data: r.data, headers: r.headers as Record<string, string> };
}

export async function run(argv: string[]): Promise<void> {
  const program = new Command();
  program
    .name("traffic-cli")
    .description("Send a request to the canary-release-mgmt edge with optional x-canary header");

  program.command("order")
    .description("POST /api/orders via the kind ingress")
    .option("--url <url>", "ingress base URL", "http://localhost:8080")
    .option("--canary", "send the x-canary: true header", false)
    .option("--user <id>", "userId", "u1")
    .option("--sku <sku>", "SKU", "sku-1")
    .option("--quantity <n>", "quantity", (v) => parseInt(v, 10), 1)
    .option("--amount <n>", "amount", (v) => parseInt(v, 10), 100)
    .action(async (cmdOpts: { url: string; canary: boolean; user: string; sku: string; quantity: number; amount: number }) => {
      const r = await sendOrder(cmdOpts);
      process.stdout.write(JSON.stringify({
        request: { url: `${cmdOpts.url}/api/orders`, canary: cmdOpts.canary },
        response: { status: r.status, data: r.data },
      }, null, 2) + "\n");
      if (r.status >= 400) process.exit(1);
    });

  await program.parseAsync(argv);
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
pnpm --filter @canary/traffic-cli test
```

Expected: 3 tests PASS.

- [ ] **Step 5: Build and verify `--help`**

```bash
pnpm --filter @canary/traffic-cli build
node tools/traffic-cli/bin/traffic-cli --help
node tools/traffic-cli/bin/traffic-cli order --help
```

Expected: commander-rendered help.

- [ ] **Step 6: Commit**

```bash
git add tools/traffic-cli/src tools/traffic-cli/test
git commit -m "feat(traffic-cli): single-request driver for /api/orders with optional x-canary header"
```

---

## Task 13: Bats smoke test (`tests/canary/canary-ctl.bats`)

**Files:**
- Create: `tests/canary/canary-ctl.bats`
- Create: `tests/canary/helpers.bash`

This task adds a real-cluster smoke test. Steps assume `make up && make build-services && make build-images && make load-images && make deploy-services` has run successfully.

- [ ] **Step 1: Write `tests/canary/helpers.bash`**

```bash
# tests/canary/helpers.bash
# Shared helpers for canary-ctl smoke tests.

CANARY_CTL="node tools/canary-ctl/bin/canary-ctl"
TRAFFIC_CLI="node tools/traffic-cli/bin/traffic-cli"
STATE_DIR="${BATS_TMPDIR:-/tmp}/canary-ctl-state"

setup_canary_state_dir() {
  rm -rf "$STATE_DIR"
  mkdir -p "$STATE_DIR"
}

teardown_canary_state_dir() {
  rm -rf "$STATE_DIR"
}

canary_ctl() {
  $CANARY_CTL --state-dir "$STATE_DIR" --repo-root "$PWD" "$@"
}
```

- [ ] **Step 2: Write `tests/canary/canary-ctl.bats`**

```bash
#!/usr/bin/env bats
# tests/canary/canary-ctl.bats
# Smoke test: full canary lifecycle against a real kind cluster.
# Pre-req: make up && make build-services && make build-images && make load-images && make deploy-services

load helpers

setup_file() {
  setup_canary_state_dir
  # Build the tools (idempotent if already built).
  pnpm --filter @canary/canary-ctl build >/dev/null
  pnpm --filter @canary/traffic-cli build >/dev/null
}

teardown_file() {
  # Best-effort cleanup in case a test left state behind.
  canary_ctl rollback payment-service >/dev/null 2>&1 || true
  teardown_canary_state_dir
}

@test "status on a clean cluster reports no canary" {
  run canary_ctl status payment-service
  [ "$status" -eq 0 ]
  [[ "$output" == *"state file: absent"* ]]
  [[ "$output" == *"helm release payment-service-canary: absent"* ]]
  [[ "$output" == *"drift: none"* ]]
}

@test "deploy-canary payment-service dev succeeds, header rule applied, state=active" {
  run canary_ctl deploy-canary payment-service dev
  [ "$status" -eq 0 ]

  # State file written.
  run canary_ctl status payment-service
  [ "$status" -eq 0 ]
  [[ "$output" == *"state file: active"* ]]
  [[ "$output" == *"virtualservice header rule: present"* ]]
  [[ "$output" == *"drift: none"* ]]
}

@test "traffic-cli order --canary returns 2xx end-to-end" {
  run $TRAFFIC_CLI order --canary --user u-smoke
  [ "$status" -eq 0 ]
  [[ "$output" == *"\"status\": 2"* ]]  # status 2xx
}

@test "rollback removes header rule, uninstalls release, clears state" {
  run canary_ctl rollback payment-service
  [ "$status" -eq 0 ]

  run canary_ctl status payment-service
  [ "$status" -eq 0 ]
  [[ "$output" == *"state file: absent"* ]]
  [[ "$output" == *"helm release payment-service-canary: absent"* ]]
  [[ "$output" == *"drift: none"* ]]
}

@test "deploy-canary with bad image tag auto-rolls back, status clean, exit nonzero" {
  run canary_ctl deploy-canary payment-service nope-tag-does-not-exist
  [ "$status" -ne 0 ]

  run canary_ctl status payment-service
  [ "$status" -eq 0 ]
  [[ "$output" == *"state file: absent"* ]]
  [[ "$output" == *"helm release payment-service-canary: absent"* ]]
  [[ "$output" == *"drift: none"* ]]
}
```

- [ ] **Step 3: Make the bats file readable by `bats` (no shebang exec needed — bats executes it)**

```bash
ls -la tests/canary/
```

Expected: helpers.bash and canary-ctl.bats present.

- [ ] **Step 4: Defer running the test — Task 14 wires the Makefile target. End-to-end execution happens after the operator manual run in Task 16.**

- [ ] **Step 5: Commit**

```bash
git add tests/canary/
git commit -m "test(canary): bats smoke test covering full deploy/rollback/auto-rollback lifecycle"
```

---

## Task 14: Makefile targets

**Files:**
- Modify: `Makefile`

- [ ] **Step 1: Append canary-ctl Make targets to `Makefile`**

Add to the existing `Makefile` (preserve all existing targets; append these to the end of the `.PHONY` list and to the targets section):

```makefile
.PHONY: canary-deploy canary-rollback canary-status canary-reconcile smoke-canary

canary-deploy: ## Deploy a canary: SVC=<service> TAG=<image-tag>
	@if [ -z "$(SVC)" ] || [ -z "$(TAG)" ]; then \
	  echo "usage: make canary-deploy SVC=<service> TAG=<tag>" >&2; exit 2; \
	fi
	@node tools/canary-ctl/bin/canary-ctl deploy-canary $(SVC) $(TAG)

canary-rollback: ## Rollback a canary: SVC=<service>
	@if [ -z "$(SVC)" ]; then echo "usage: make canary-rollback SVC=<service>" >&2; exit 2; fi
	@node tools/canary-ctl/bin/canary-ctl rollback $(SVC)

canary-status: ## Show canary status: SVC=<service>
	@if [ -z "$(SVC)" ]; then echo "usage: make canary-status SVC=<service>" >&2; exit 2; fi
	@node tools/canary-ctl/bin/canary-ctl status $(SVC)

canary-reconcile: ## Reconcile canary state for SVC=<service>
	@if [ -z "$(SVC)" ]; then echo "usage: make canary-reconcile SVC=<service>" >&2; exit 2; fi
	@node tools/canary-ctl/bin/canary-ctl reconcile $(SVC)

smoke-canary: ## Run canary-ctl bats smoke tests (requires deployed substrate)
	@pnpm --filter @canary/canary-ctl build >/dev/null
	@pnpm --filter @canary/traffic-cli build >/dev/null
	@bats tests/canary/canary-ctl.bats
```

- [ ] **Step 2: Verify `make help` shows the new targets**

```bash
make help | grep canary
```

Expected: 5 lines — `canary-deploy`, `canary-rollback`, `canary-status`, `canary-reconcile`, `smoke-canary`.

- [ ] **Step 3: Commit**

```bash
git add Makefile
git commit -m "feat(make): canary-deploy/rollback/status/reconcile + smoke-canary targets"
```

---

## Task 15: README — Plan 1.4 section

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Append a "Plan 1.4 — canary-ctl + traffic-cli" section to `README.md`**

Append to the bottom of `README.md`:

```markdown

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
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs(readme): add Plan 1.4 section (canary-ctl + traffic-cli)"
```

---

## Task 16: End-to-end manual verification + smoke run

**Files:** none modified — verification only.

- [ ] **Step 1: Tear down any prior cluster state and re-bootstrap**

```bash
make down
make up
make build-services
make build-images && make load-images
make deploy-services
make smoke-services
```

Expected: 1.3.b smoke passes.

- [ ] **Step 2: Run unit tests for both tools**

```bash
pnpm --filter @canary/canary-ctl test
pnpm --filter @canary/traffic-cli test
```

Expected: all unit tests pass (~28 total across both packages).

- [ ] **Step 3: Run `make verify`**

```bash
make verify
```

Expected: all platform + service + tool tests pass.

- [ ] **Step 4: Manual canary deploy + rollback against the cluster**

```bash
make canary-deploy SVC=payment-service TAG=dev
make canary-status SVC=payment-service
node tools/traffic-cli/bin/traffic-cli order --canary
node tools/traffic-cli/bin/traffic-cli order            # no header
make canary-rollback SVC=payment-service
make canary-status SVC=payment-service
```

Expected:
- `canary-deploy` exits 0 within 60–120 s.
- `status` after deploy: `state file: active`, `header rule: present`, `drift: none`.
- `traffic-cli` returns 2xx both times.
- (Optional manual check) Kiali at http://localhost:20001 shows the `--canary` request hitting the `payment-service-canary` workload.
- `rollback` exits 0 within 30 s.
- `status` after rollback: `state file: absent`, `header rule: absent`, `drift: none`.

- [ ] **Step 5: Run the bats smoke test**

```bash
make smoke-canary
```

Expected: all 5 bats assertions PASS within ~3 minutes.

- [ ] **Step 6: No commit — verification task only**

If any step in this task fails, return to the relevant earlier task and fix the underlying code.

---

## Self-review checklist (run after task definitions)

- **Spec coverage.** Each spec section maps to at least one task: Goals → Tasks 7–10 (4 commands) + 12 (traffic-cli); Architecture → Tasks 1, 11; Service registry → Task 2; State file → Task 3; VS header rule → Task 5 (patch builders) + 7/8/10 (apply); Helm release → Task 6; Error handling [A][B][F][G] → Tasks 7 (auto-rollback), 8 (rollback ordering), 10 (reconcile decision table); Testing strategy: unit → Tasks 2–10/12; smoke → Task 13; manual → Task 16; Done-when → Task 16.
- **Placeholders.** None. All file contents are concrete.
- **Type/name consistency.** `ServiceEntry`, `CanaryState`, `CanaryPhase` exported from `registry.ts`/`state.ts` and consumed by all command modules. `buildHeaderRulePatch` / `buildDefaultOnlyPatch` / `patchVirtualService` / `getDeploymentReady` / `getVirtualServiceRules` named identically in `kubectl.ts` and all test imports. `upgradeInstallCanary` / `uninstallCanary` / `listReleases` named identically in `helm.ts` and test imports. Commander option names (`--state-dir`, `--repo-root`, `--grace-seconds`, `--verbose`, `--json`, `--adopt`) match the spec's "global flags" list.
- **TDD discipline.** Every task with code follows the failing-test → run → implement → run → commit cycle. Tasks 4 (exec), 11 (CLI wiring), and 14/15 (Makefile/README) are not test-gated because they're glue; their correctness is proven by the task that consumes them next or by Task 16's manual verification.
- **Frequent commits.** 14 commits across Tasks 1–15 (Task 16 makes none). Each commit produces a working state.

---

## Done when

- All unit tests green: `pnpm --filter @canary/canary-ctl test` and `pnpm --filter @canary/traffic-cli test`.
- `make verify` passes (platform + services + tools).
- `make smoke-canary` passes against a fresh `make up && make deploy-services` cluster.
- Operator can run `make canary-deploy SVC=payment-service TAG=dev`, see the canary in `make canary-status`, drive a `traffic-cli order --canary` request, and roll back cleanly.
- All commits in this task list are present on the working branch.
- README documents the Plan 1.4 operator workflow.
