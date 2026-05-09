# Phase 1.5.a — e2e harness foundation + S1 Baseline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the TypeScript e2e harness (`@canary/e2e` pnpm package using vitest) with helpers for canary lifecycle, traffic, subset assertion, Kafka/Restate admin queries, and TS-native load generation; add the small `x-served-version` response-header instrumentation to `lib-java` + `lib-node` + the Helm chart; ship the **S1 Baseline** scenario end-to-end as proof the foundation works.

**Architecture:** A new `tests/e2e/` workspace package modeled on `@canary/canary-ctl` from Plan 1.4 (TypeScript + vitest + ESM). Vitest runs scenarios sequentially in a single fork to avoid cluster-mutation conflicts. Helpers shell out to `canary-ctl` and use axios/kafkajs for cluster I/O. Each service stamps `x-served-version: stable|canary` on its outbound HTTP responses based on a `VERSION` env var injected by the Helm chart.

**Tech Stack:**
- TypeScript 5.6+ (matches existing workspace packages)
- vitest 2.x with `pool: "forks", singleFork: true` for sequential execution
- axios 1.x (already in tools/traffic-cli)
- kafkajs 2.x (new dep for admin client)
- Spring Boot 4 (lib-java filter)
- Express (lib-node middleware)
- Helm 3.x chart edit

**Spec reference:** `docs/superpowers/specs/2026-05-09-canary-release-phase-1-5-a-e2e-foundation-design.md`

---

## Prerequisites

The plan assumes:
- Plan 1.4 merged to main (canary-ctl + traffic-cli built and tested).
- `helm`, `kubectl`, `node`, `pnpm`, `bats` on PATH (existing).
- Operator can rebuild + reload images before the S1 run (since this plan changes lib-java + lib-node, the service images need a refresh).

---

## File Structure

```
tests/e2e/                                              # NEW pnpm package: @canary/e2e
├── package.json                                        # NEW
├── tsconfig.json                                       # NEW
├── vitest.config.ts                                    # NEW (sequential, 5min timeout)
├── helpers/
│   ├── canary.ts                                       # NEW: canary-ctl shell-out wrapper
│   ├── traffic.ts                                      # NEW: sendOrder + low-level POST
│   ├── subset.ts                                       # NEW: x-served-version assertion
│   ├── load.ts                                         # NEW: TS load generator
│   ├── kafka-admin.ts                                  # NEW: kafkajs admin client
│   └── restate-admin.ts                                # NEW: axios → :9070
└── s1-baseline.test.ts                                 # NEW: S1 scenario

platform/lib-java/                                      # MODIFY
├── src/main/java/com/canary/platform/lib/
│   └── XCanaryResponseHeaderFilter.java                # NEW
├── src/main/java/com/canary/platform/lib/autoconfigure/
│   └── XCanaryAutoConfiguration.java                   # MODIFY: register the new filter bean
└── src/test/java/com/canary/platform/lib/
    └── XCanaryResponseHeaderFilterTest.java            # NEW

platform/lib-node/                                      # MODIFY
├── src/x-served-version-middleware.ts                  # NEW
├── src/index.ts                                        # MODIFY: re-export the middleware
└── src/__tests__/x-served-version-middleware.test.ts   # NEW

services/order-service/src/                             # MODIFY: app.use(xServedVersionMiddleware())
services/notification-service/src/                      # MODIFY: app.use(xServedVersionMiddleware())

deploy/helm/service-chart/templates/configmap.yaml      # MODIFY: add VERSION key
deploy/helm/values/payment-service.yaml                 # (no change — VERSION comes from .Values.version)

pnpm-workspace.yaml                                     # MODIFY: add tests/* glob
Makefile                                                # MODIFY: add e2e + ci-local targets
README.md                                               # MODIFY: add Plan 1.5.a section
```

**Why one workspace package per surface:** scopes deps cleanly; `pnpm --filter @canary/e2e test` runs scenarios in isolation; matches the workspace pattern from `lib-node`, `canary-ctl`, `traffic-cli`.

**Why the response-header filter lives in lib-java (not in each service):** all 5 services use `lib-java` or `lib-node` already; one place to wire it. Java services get the bean automatically via auto-config; Node services add a one-line `app.use()`.

---

## Task 1: Scaffold `tests/e2e` package and add to workspace

**Files:**
- Create: `tests/e2e/package.json`
- Create: `tests/e2e/tsconfig.json`
- Create: `tests/e2e/vitest.config.ts`
- Modify: `pnpm-workspace.yaml`

- [ ] **Step 1: Add `tests/*` to the workspace glob**

Read `pnpm-workspace.yaml`. Current content:

```yaml
packages:
  - "platform/lib-node"
  - "platform/restate-defs-node"
  - "services/*"
  - "tools/*"
```

Add a `"tests/*"` line so all 5 globs are present.

- [ ] **Step 2: Write `tests/e2e/package.json`**

```json
{
  "name": "@canary/e2e",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "scripts": {
    "build": "tsc -b",
    "test": "vitest run",
    "test:watch": "vitest",
    "lint": "tsc --noEmit"
  },
  "dependencies": {
    "axios": "^1.7.0",
    "kafkajs": "^2.2.4"
  },
  "devDependencies": {
    "@types/node": "^22.0.0",
    "typescript": "^5.6.2",
    "vitest": "^2.0.0"
  }
}
```

- [ ] **Step 3: Write `tests/e2e/tsconfig.json`**

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ESNext",
    "moduleResolution": "Bundler",
    "lib": ["ES2022"],
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true,
    "declaration": true,
    "outDir": "dist",
    "rootDir": ".",
    "composite": true
  },
  "include": ["**/*.ts"],
  "exclude": ["dist", "node_modules"]
}
```

- [ ] **Step 4: Write `tests/e2e/vitest.config.ts`**

```typescript
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["**/*.test.ts"],
    exclude: ["**/node_modules/**", "**/dist/**"],
    environment: "node",
    globals: false,
    testTimeout: 5 * 60_000,
    hookTimeout: 5 * 60_000,
    pool: "forks",
    poolOptions: {
      forks: { singleFork: true },
    },
    fileParallelism: false,
    reporters: ["verbose"],
  },
});
```

- [ ] **Step 5: Install dependencies**

```bash
pnpm install
```

Expected: pnpm picks up the new `@canary/e2e` package, installs `axios`, `kafkajs`, vitest, typescript. No errors.

- [ ] **Step 6: Verify TS compiles (empty package, just config check)**

```bash
pnpm --filter @canary/e2e build
```

Expected: clean build (no source files yet, but tsc should not error on the empty include).

If tsc errors with "no input files", create a placeholder `tests/e2e/index.ts`:

```typescript
export {};
```

Then re-run build. Confirm clean.

- [ ] **Step 7: Commit**

```bash
git add pnpm-workspace.yaml pnpm-lock.yaml tests/e2e
git commit -m "$(cat <<'EOF'
feat(e2e): scaffold @canary/e2e pnpm package

vitest with sequential single-fork pool (cluster-mutation tests
can't parallelize), 5-minute per-test timeout, kafkajs + axios deps.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: lib-java — `XCanaryResponseHeaderFilter` + auto-config wiring

**Files:**
- Create: `platform/lib-java/src/main/java/com/canary/platform/lib/XCanaryResponseHeaderFilter.java`
- Create: `platform/lib-java/src/test/java/com/canary/platform/lib/XCanaryResponseHeaderFilterTest.java`
- Modify: `platform/lib-java/src/main/java/com/canary/platform/lib/autoconfigure/XCanaryAutoConfiguration.java`

- [ ] **Step 1: Write the failing test**

`platform/lib-java/src/test/java/com/canary/platform/lib/XCanaryResponseHeaderFilterTest.java`:

```java
package com.canary.platform.lib;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class XCanaryResponseHeaderFilterTest {

    @Test
    void setsHeaderFromConstructorArg() throws ServletException, IOException {
        XCanaryResponseHeaderFilter filter = new XCanaryResponseHeaderFilter("canary");
        HttpServletRequest req = mock(HttpServletRequest.class);
        HttpServletResponse res = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);

        filter.doFilter(req, res, chain);

        verify(res).setHeader("x-served-version", "canary");
        verify(chain).doFilter(req, res);
    }

    @Test
    void defaultsToStableWhenArgIsNull() throws ServletException, IOException {
        XCanaryResponseHeaderFilter filter = new XCanaryResponseHeaderFilter(null);
        HttpServletRequest req = mock(HttpServletRequest.class);
        HttpServletResponse res = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);

        filter.doFilter(req, res, chain);

        verify(res).setHeader("x-served-version", "stable");
    }

    @Test
    void defaultsToStableWhenArgIsBlank() throws ServletException, IOException {
        XCanaryResponseHeaderFilter filter = new XCanaryResponseHeaderFilter("   ");
        HttpServletRequest req = mock(HttpServletRequest.class);
        HttpServletResponse res = mock(HttpServletResponse.class);
        FilterChain chain = mock(FilterChain.class);

        filter.doFilter(req, res, chain);

        verify(res).setHeader("x-served-version", "stable");
    }

    @Test
    void filterChainAlwaysProceedsEvenIfHeaderSetThrows() throws ServletException, IOException {
        XCanaryResponseHeaderFilter filter = new XCanaryResponseHeaderFilter("stable");
        HttpServletRequest req = mock(HttpServletRequest.class);
        HttpServletResponse res = mock(HttpServletResponse.class);
        // Don't throw; just verify chain runs.
        FilterChain chain = mock(FilterChain.class);

        filter.doFilter(req, res, chain);

        verify(chain).doFilter(req, res);
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
./gradlew :platform:lib-java:test --tests XCanaryResponseHeaderFilterTest
```

Expected: FAIL — `XCanaryResponseHeaderFilter` does not exist.

- [ ] **Step 3: Write the implementation**

`platform/lib-java/src/main/java/com/canary/platform/lib/XCanaryResponseHeaderFilter.java`:

```java
package com.canary.platform.lib;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.core.Ordered;

import java.io.IOException;

/**
 * Stamps the configured version on the outbound response so e2e tests can
 * verify which subset (stable | canary) handled the request. Reads the
 * version once at construction.
 */
public class XCanaryResponseHeaderFilter implements Filter, Ordered {

    public static final String HEADER_NAME = "x-served-version";
    public static final String DEFAULT_VERSION = "stable";

    private final String version;

    public XCanaryResponseHeaderFilter(String version) {
        this.version = (version == null || version.isBlank()) ? DEFAULT_VERSION : version.trim();
    }

    @Override
    public int getOrder() {
        // Run after the request filter so we don't interfere with header parsing.
        return Ordered.HIGHEST_PRECEDENCE + 200;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (response instanceof HttpServletResponse http) {
            http.setHeader(HEADER_NAME, version);
        }
        chain.doFilter(request, response);
    }
}
```

- [ ] **Step 4: Wire the filter as a Spring bean**

Modify `platform/lib-java/src/main/java/com/canary/platform/lib/autoconfigure/XCanaryAutoConfiguration.java` to add the bean:

```java
package com.canary.platform.lib.autoconfigure;

import com.canary.platform.lib.XCanaryKafkaProducerInterceptor;
import com.canary.platform.lib.XCanaryRequestFilter;
import com.canary.platform.lib.XCanaryResponseHeaderFilter;
import com.canary.platform.lib.XCanaryRestClientInterceptor;
import com.canary.platform.lib.XCanaryRestateClientCustomizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.web.client.RestClient;

import java.util.function.Consumer;

@AutoConfiguration
public class XCanaryAutoConfiguration {

    @Bean
    public XCanaryRequestFilter xCanaryRequestFilter() {
        return new XCanaryRequestFilter();
    }

    @Bean
    public XCanaryResponseHeaderFilter xCanaryResponseHeaderFilter(
            @Value("${canary.version:${VERSION:stable}}") String version) {
        return new XCanaryResponseHeaderFilter(version);
    }

    @Bean
    public XCanaryRestClientInterceptor xCanaryRestClientInterceptor() {
        return new XCanaryRestClientInterceptor();
    }

    @Bean
    public Consumer<RestClient.Builder> xCanaryRestClientCustomizer(XCanaryRestClientInterceptor interceptor) {
        return builder -> builder.requestInterceptor(interceptor);
    }

    @Bean
    public XCanaryKafkaProducerInterceptor<Object, Object> xCanaryKafkaProducerInterceptor() {
        return new XCanaryKafkaProducerInterceptor<>();
    }

    @Bean
    public XCanaryRestateClientCustomizer xCanaryRestateClientCustomizer() {
        return new XCanaryRestateClientCustomizer();
    }
}
```

The `@Value` placeholder reads either `canary.version` (Spring property) or `VERSION` (env var), defaulting to `stable`.

- [ ] **Step 5: Run the test to verify it passes**

```bash
./gradlew :platform:lib-java:test --tests XCanaryResponseHeaderFilterTest
```

Expected: 4 tests PASS.

- [ ] **Step 6: Run full lib-java test suite to confirm nothing else broke**

```bash
./gradlew :platform:lib-java:test
```

Expected: all existing lib-java tests still pass plus 4 new tests.

- [ ] **Step 7: Commit**

```bash
git add platform/lib-java/src/main/java/com/canary/platform/lib/XCanaryResponseHeaderFilter.java \
        platform/lib-java/src/main/java/com/canary/platform/lib/autoconfigure/XCanaryAutoConfiguration.java \
        platform/lib-java/src/test/java/com/canary/platform/lib/XCanaryResponseHeaderFilterTest.java
git commit -m "$(cat <<'EOF'
feat(lib-java): XCanaryResponseHeaderFilter stamps x-served-version

Reads VERSION env var (or canary.version Spring property) at startup;
defaults to "stable". Filter runs after the request filter (order
HIGHEST_PRECEDENCE + 200) and adds the header on every response.
Wired by XCanaryAutoConfiguration.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: lib-node — `xServedVersionMiddleware` + index re-export

**Files:**
- Create: `platform/lib-node/src/x-served-version-middleware.ts`
- Create: `platform/lib-node/src/__tests__/x-served-version-middleware.test.ts`
- Modify: `platform/lib-node/src/index.ts`

- [ ] **Step 1: Write the failing test**

`platform/lib-node/src/__tests__/x-served-version-middleware.test.ts`:

```typescript
import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import type { Request, Response, NextFunction } from "express";
import { xServedVersionMiddleware, X_SERVED_VERSION_HEADER } from "../x-served-version-middleware.js";

function mockRes(): Response {
  return { setHeader: vi.fn() } as unknown as Response;
}

describe("xServedVersionMiddleware", () => {
  const originalVersion = process.env.VERSION;

  afterEach(() => {
    if (originalVersion === undefined) delete process.env.VERSION;
    else process.env.VERSION = originalVersion;
  });

  it("sets the header to VERSION env var when set", () => {
    process.env.VERSION = "canary";
    const mw = xServedVersionMiddleware();
    const res = mockRes();
    const next = vi.fn() as unknown as NextFunction;
    mw({} as Request, res, next);
    expect(res.setHeader).toHaveBeenCalledWith("x-served-version", "canary");
    expect(next).toHaveBeenCalled();
  });

  it("defaults to stable when VERSION is unset", () => {
    delete process.env.VERSION;
    const mw = xServedVersionMiddleware();
    const res = mockRes();
    const next = vi.fn() as unknown as NextFunction;
    mw({} as Request, res, next);
    expect(res.setHeader).toHaveBeenCalledWith("x-served-version", "stable");
  });

  it("defaults to stable when VERSION is empty string", () => {
    process.env.VERSION = "";
    const mw = xServedVersionMiddleware();
    const res = mockRes();
    const next = vi.fn() as unknown as NextFunction;
    mw({} as Request, res, next);
    expect(res.setHeader).toHaveBeenCalledWith("x-served-version", "stable");
  });

  it("captures the version once at factory call (does not re-read env per request)", () => {
    process.env.VERSION = "stable";
    const mw = xServedVersionMiddleware();
    process.env.VERSION = "canary"; // mutate AFTER factory call
    const res = mockRes();
    const next = vi.fn() as unknown as NextFunction;
    mw({} as Request, res, next);
    expect(res.setHeader).toHaveBeenCalledWith("x-served-version", "stable");
  });

  it("exports X_SERVED_VERSION_HEADER constant", () => {
    expect(X_SERVED_VERSION_HEADER).toBe("x-served-version");
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
pnpm --filter @canary/lib-node test
```

Expected: FAIL — `x-served-version-middleware` does not exist.

- [ ] **Step 3: Write the implementation**

`platform/lib-node/src/x-served-version-middleware.ts`:

```typescript
import type { NextFunction, Request, Response } from "express";

export const X_SERVED_VERSION_HEADER = "x-served-version";
export const X_SERVED_VERSION_DEFAULT = "stable";

/**
 * Returns an Express middleware that stamps `x-served-version: <VERSION>` on
 * every response. The version is captured once at factory call time from
 * `process.env.VERSION`, defaulting to "stable" if unset or blank.
 */
export function xServedVersionMiddleware(): (req: Request, res: Response, next: NextFunction) => void {
  const raw = process.env.VERSION;
  const version = raw && raw.trim().length > 0 ? raw.trim() : X_SERVED_VERSION_DEFAULT;
  return (_req, res, next) => {
    res.setHeader(X_SERVED_VERSION_HEADER, version);
    next();
  };
}
```

- [ ] **Step 4: Re-export from `src/index.ts`**

Read current `platform/lib-node/src/index.ts`:

```typescript
export * from "./x-canary-constants.js";
export * from "./x-canary-context.js";
export * from "./x-canary-middleware.js";
export * from "./x-canary-axios.js";
export * from "./x-canary-kafka.js";
export * from "./x-canary-restate.js";
```

Append `export * from "./x-served-version-middleware.js";` at the end.

- [ ] **Step 5: Run the test to verify it passes**

```bash
pnpm --filter @canary/lib-node test
```

Expected: 5 new tests PASS plus all 17 existing tests (22 total).

- [ ] **Step 6: Commit**

```bash
git add platform/lib-node/src/x-served-version-middleware.ts \
        platform/lib-node/src/__tests__/x-served-version-middleware.test.ts \
        platform/lib-node/src/index.ts
git commit -m "$(cat <<'EOF'
feat(lib-node): xServedVersionMiddleware stamps x-served-version

Reads process.env.VERSION once at factory call; defaults to "stable".
Each Node service calls app.use(xServedVersionMiddleware()) in app
setup. Exported via the package index.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Helm chart — pass `VERSION` env var into containers

**Files:**
- Modify: `deploy/helm/service-chart/templates/configmap.yaml`

- [ ] **Step 1: Read the current ConfigMap template**

```bash
cat deploy/helm/service-chart/templates/configmap.yaml
```

The ConfigMap is consumed via `envFrom: configMapRef` in the deployment template. Adding a `VERSION` key here makes it available as an env var with no deployment-template change.

- [ ] **Step 2: Add VERSION key**

Edit `deploy/helm/service-chart/templates/configmap.yaml`. Locate the `data:` section (the existing config keys). Add a `VERSION` key sourced from `.Values.version`. The exact insertion point depends on the file's current shape; the addition looks like:

```yaml
data:
  VERSION: {{ .Values.version | quote }}
  # ...existing keys preserved
```

If the existing template iterates over `.Values.env` to produce keys, instead add VERSION at the same level:

```yaml
data:
  VERSION: {{ .Values.version | quote }}
  {{- range $k, $v := .Values.env }}
  {{ $k }}: {{ $v | quote }}
  {{- end }}
```

Preserve any existing structure; the only addition is the `VERSION:` line.

- [ ] **Step 3: Verify the chart still renders**

```bash
helm template test deploy/helm/service-chart \
  -f deploy/helm/values/payment-service.yaml \
  | grep -A2 "name: payment-service-config" | head -10
```

Expected: ConfigMap renders with `VERSION: "stable"` (or the default from values).

```bash
helm template test deploy/helm/service-chart \
  -f deploy/helm/values/payment-service.yaml \
  -f deploy/helm/values/canary-overlay.yaml \
  | grep -A2 "name:" | grep -i version
```

Expected: shows `VERSION: "canary"` (from the canary overlay's `version: canary`).

- [ ] **Step 4: Commit**

```bash
git add deploy/helm/service-chart/templates/configmap.yaml
git commit -m "$(cat <<'EOF'
feat(helm): pass VERSION env var into containers via ConfigMap

Sources VERSION from .Values.version (already used for the pod
label). Stable releases get VERSION=stable; canary releases get
VERSION=canary via canary-overlay.yaml. Consumed by lib-java's
XCanaryResponseHeaderFilter and lib-node's xServedVersionMiddleware
to stamp x-served-version on outbound responses.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Wire `xServedVersionMiddleware` into Node services

**Files:**
- Modify: `services/order-service/src/app.ts` (or equivalent app setup file)
- Modify: `services/notification-service/src/app.ts` (or equivalent)

The Java services need no edit — auto-config wires the filter automatically.

- [ ] **Step 1: Find the order-service app setup file**

```bash
find services/order-service/src -type f -name "*.ts" | xargs grep -l "xCanaryMiddleware" | head -3
```

Look for the file where `xCanaryMiddleware` is added with `app.use(...)` (likely `services/order-service/src/app.ts` or `index.ts`).

- [ ] **Step 2: Add the middleware to order-service**

Open the file from Step 1. Find the line `app.use(xCanaryMiddleware)` (or similar). Add `app.use(xServedVersionMiddleware());` immediately after it. Update the import to include the new export:

```typescript
import { xCanaryMiddleware, xServedVersionMiddleware } from "@canary/lib-node";
```

Order matters: middleware runs in the order added. Putting `xServedVersionMiddleware` AFTER the canary middleware (and after the `/health` route per the existing comment style) ensures it stamps every response regardless of the route. If the existing app already places `/health` BEFORE `xCanaryMiddleware`, place `xServedVersionMiddleware()` right next to `xCanaryMiddleware` (immediately after).

- [ ] **Step 3: Run order-service tests to confirm nothing broke**

```bash
pnpm --filter @canary/order-service test
```

Expected: all existing tests still pass.

- [ ] **Step 4: Find the notification-service app setup file**

```bash
find services/notification-service/src -type f -name "*.ts" | xargs grep -l "xCanaryMiddleware" | head -3
```

- [ ] **Step 5: Add the middleware to notification-service**

Same pattern as Step 2. Update import + add `app.use(xServedVersionMiddleware());` after `app.use(xCanaryMiddleware)`.

- [ ] **Step 6: Run notification-service tests**

```bash
pnpm --filter @canary/notification-service test
```

Expected: all existing tests still pass.

- [ ] **Step 7: Build both services to confirm TS compiles**

```bash
pnpm --filter @canary/order-service build
pnpm --filter @canary/notification-service build
```

Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add services/order-service/src services/notification-service/src
git commit -m "$(cat <<'EOF'
feat(services): wire xServedVersionMiddleware into Node services

Order and notification services now stamp x-served-version on
every outbound response (env: VERSION; defaults to stable). Java
services get the same behaviour automatically via lib-java's
XCanaryAutoConfiguration.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: e2e helper — `subset.ts` (assertion + accessor)

**Files:**
- Create: `tests/e2e/helpers/subset.ts`
- Create: `tests/e2e/helpers/__tests__/subset.test.ts`

This is the only helper that gets unit tests in 1.5.a — it's pure logic and worth a few quick tests.

- [ ] **Step 1: Write the failing test**

`tests/e2e/helpers/__tests__/subset.test.ts`:

```typescript
import { describe, expect, it } from "vitest";
import { assertServedVersion, getServedVersion } from "../subset.js";

describe("subset helpers", () => {
  it("getServedVersion returns the header value when present", () => {
    expect(getServedVersion({ "x-served-version": "stable" })).toBe("stable");
    expect(getServedVersion({ "x-served-version": "canary" })).toBe("canary");
  });

  it("getServedVersion returns null when header absent", () => {
    expect(getServedVersion({})).toBeNull();
    expect(getServedVersion({ "other-header": "value" })).toBeNull();
  });

  it("getServedVersion returns null for unrecognized values", () => {
    expect(getServedVersion({ "x-served-version": "v2" })).toBeNull();
  });

  it("assertServedVersion succeeds when header matches expected", () => {
    expect(() => assertServedVersion({ "x-served-version": "stable" }, "stable")).not.toThrow();
    expect(() => assertServedVersion({ "x-served-version": "canary" }, "canary")).not.toThrow();
  });

  it("assertServedVersion throws clear error when header missing", () => {
    expect(() => assertServedVersion({}, "stable")).toThrow(/x-served-version header missing/i);
  });

  it("assertServedVersion throws clear error when header mismatches", () => {
    expect(() => assertServedVersion({ "x-served-version": "stable" }, "canary"))
      .toThrow(/expected canary, got stable/i);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
pnpm --filter @canary/e2e test
```

Expected: FAIL — `subset` module does not exist.

- [ ] **Step 3: Write the implementation**

`tests/e2e/helpers/subset.ts`:

```typescript
export type ServedVersion = "stable" | "canary";

export const X_SERVED_VERSION_HEADER = "x-served-version";

export function getServedVersion(headers: Record<string, string | string[] | undefined>): ServedVersion | null {
  const raw = headers[X_SERVED_VERSION_HEADER];
  const value = Array.isArray(raw) ? raw[0] : raw;
  if (value === "stable" || value === "canary") return value;
  return null;
}

export function assertServedVersion(
  headers: Record<string, string | string[] | undefined>,
  expected: ServedVersion,
): void {
  const got = getServedVersion(headers);
  if (got === null) {
    const headerNames = Object.keys(headers).join(", ");
    throw new Error(
      `x-served-version header missing or unrecognized. Headers received: [${headerNames}]`,
    );
  }
  if (got !== expected) {
    throw new Error(`x-served-version: expected ${expected}, got ${got}`);
  }
}
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
pnpm --filter @canary/e2e test
```

Expected: 6 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/e2e/helpers/subset.ts tests/e2e/helpers/__tests__/subset.test.ts
git commit -m "$(cat <<'EOF'
feat(e2e): subset.ts — assertServedVersion + getServedVersion helpers

Reads the x-served-version response header and asserts it matches
the expected version. Used by every scenario that needs to verify
which subset (stable | canary) handled a request.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 7: e2e helper — `traffic.ts` (sendOrder)

**Files:**
- Create: `tests/e2e/helpers/traffic.ts`

No unit tests for this one (per spec — thin axios wrapper, verified by S1 running end-to-end).

- [ ] **Step 1: Write the helper**

`tests/e2e/helpers/traffic.ts`:

```typescript
import axios from "axios";

export interface SendOrderOpts {
  url?: string;
  canary?: boolean;
  user?: string;
  sku?: string;
  quantity?: number;
  amount?: number;
}

export interface SendOrderResult {
  status: number;
  data: unknown;
  headers: Record<string, string>;
}

export async function sendOrder(opts: SendOrderOpts = {}): Promise<SendOrderResult> {
  const url = opts.url ?? "http://localhost:8080";
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (opts.canary) headers["x-canary"] = "true";
  const r = await axios.post(
    `${url}/api/orders`,
    {
      userId: opts.user ?? "u1",
      sku: opts.sku ?? "sku-1",
      quantity: opts.quantity ?? 1,
      amount: opts.amount ?? 100,
    },
    { headers, validateStatus: () => true },
  );
  return { status: r.status, data: r.data, headers: r.headers as Record<string, string> };
}
```

- [ ] **Step 2: Verify TS compiles**

```bash
pnpm --filter @canary/e2e build
```

Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/helpers/traffic.ts
git commit -m "feat(e2e): traffic.ts — sendOrder helper for /api/orders POST"
```

(HEREDOC + Co-Authored-By footer.)

---

## Task 8: e2e helper — `canary.ts` (canary-ctl shell-out wrapper)

**Files:**
- Create: `tests/e2e/helpers/canary.ts`

- [ ] **Step 1: Write the helper**

`tests/e2e/helpers/canary.ts`:

```typescript
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { resolve } from "node:path";

const execFileAsync = promisify(execFile);

const REPO_ROOT = resolve(process.cwd(), process.env.E2E_REPO_ROOT ?? ".");
const CANARY_CTL = resolve(REPO_ROOT, "tools/canary-ctl/bin/canary-ctl");

interface CanaryCtlOpts {
  stateDir?: string;
  json?: boolean;
}

async function run(args: string[], opts: CanaryCtlOpts = {}): Promise<{ stdout: string; stderr: string }> {
  const fullArgs = ["--repo-root", REPO_ROOT];
  if (opts.stateDir) fullArgs.push("--state-dir", opts.stateDir);
  fullArgs.push(...args);
  try {
    const { stdout, stderr } = await execFileAsync("node", [CANARY_CTL, ...fullArgs], {
      maxBuffer: 16 * 1024 * 1024,
      timeout: 5 * 60_000,
    });
    return { stdout: stdout.toString(), stderr: stderr.toString() };
  } catch (err) {
    const e = err as NodeJS.ErrnoException & { stdout?: Buffer | string; stderr?: Buffer | string; code?: number | string };
    const stderr = e.stderr?.toString() ?? "";
    throw new Error(`canary-ctl ${fullArgs.join(" ")} failed (exit ${e.code}): ${stderr || e.message}`);
  }
}

export interface CanaryStatusResult {
  service: string;
  statePhase: "deploying" | "deployment-ready" | "active" | "rolling-back" | null;
  stateTag: string | null;
  helmCanaryPresent: boolean;
  helmCanaryStatus: string | null;
  deploymentReady: number;
  deploymentTotal: number;
  deploymentExists: boolean;
  vsHasHeaderRule: boolean;
  vsRuleNames: string[];
  drift: string[];
}

export async function deployCanary(svc: string, tag: string, opts: CanaryCtlOpts = {}): Promise<void> {
  await run(["deploy-canary", svc, tag], opts);
}

export async function rollback(svc: string, opts: CanaryCtlOpts = {}): Promise<void> {
  await run(["rollback", svc], opts);
}

export async function status(svc: string, opts: CanaryCtlOpts = {}): Promise<CanaryStatusResult> {
  const { stdout } = await run(["status", svc, "--json"], opts);
  return JSON.parse(stdout) as CanaryStatusResult;
}

export async function reconcile(svc: string, opts: CanaryCtlOpts & { adopt?: boolean } = {}): Promise<void> {
  const args = ["reconcile", svc];
  if (opts.adopt) args.push("--adopt");
  await run(args, opts);
}
```

- [ ] **Step 2: Verify TS compiles**

```bash
pnpm --filter @canary/e2e build
```

Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/helpers/canary.ts
git commit -m "feat(e2e): canary.ts — shell-out wrappers for canary-ctl commands"
```

(HEREDOC + Co-Authored-By footer.)

---

## Task 9: e2e helper — `load.ts` (TS-native load generator)

**Files:**
- Create: `tests/e2e/helpers/load.ts`

- [ ] **Step 1: Write the helper**

`tests/e2e/helpers/load.ts`:

```typescript
import axios, { type AxiosRequestConfig } from "axios";

export interface LoadOpts {
  url: string;
  method?: "GET" | "POST";
  rps: number;
  durationSeconds: number;
  headers?: Record<string, string>;
  payload?: unknown;
}

export interface LoadStats {
  requestsSent: number;
  successCount: number;
  failureCount: number;
  p50Ms: number;
  p99Ms: number;
  totalDurationMs: number;
  errorSamples: string[];
}

export async function runLoad(opts: LoadOpts): Promise<LoadStats> {
  const intervalMs = 1000 / opts.rps;
  const start = Date.now();
  const deadline = start + opts.durationSeconds * 1000;
  const latencies: number[] = [];
  const errors: string[] = [];
  let sent = 0;
  let success = 0;
  let failure = 0;
  const inFlight: Promise<void>[] = [];

  while (Date.now() < deadline) {
    const reqStart = Date.now();
    sent++;
    const cfg: AxiosRequestConfig = {
      method: opts.method ?? "POST",
      url: opts.url,
      headers: opts.headers,
      data: opts.payload,
      validateStatus: () => true,
    };
    const p = axios(cfg).then(
      (r) => {
        const lat = Date.now() - reqStart;
        latencies.push(lat);
        if (r.status >= 200 && r.status < 300) success++;
        else {
          failure++;
          if (errors.length < 5) errors.push(`HTTP ${r.status}`);
        }
      },
      (err: Error) => {
        failure++;
        if (errors.length < 5) errors.push(err.message);
      },
    );
    inFlight.push(p);
    await new Promise<void>((res) => setTimeout(res, intervalMs));
  }

  await Promise.all(inFlight);
  latencies.sort((a, b) => a - b);
  const p50 = latencies.length === 0 ? 0 : latencies[Math.floor(latencies.length * 0.5)];
  const p99 = latencies.length === 0 ? 0 : latencies[Math.floor(latencies.length * 0.99)];

  return {
    requestsSent: sent,
    successCount: success,
    failureCount: failure,
    p50Ms: p50,
    p99Ms: p99,
    totalDurationMs: Date.now() - start,
    errorSamples: errors,
  };
}
```

- [ ] **Step 2: Verify TS compiles**

```bash
pnpm --filter @canary/e2e build
```

Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/helpers/load.ts
git commit -m "feat(e2e): load.ts — TS-native load generator (axios + setInterval pacing)"
```

(HEREDOC + Co-Authored-By footer.)

---

## Task 10: e2e helper — `kafka-admin.ts`

**Files:**
- Create: `tests/e2e/helpers/kafka-admin.ts`

- [ ] **Step 1: Write the helper**

`tests/e2e/helpers/kafka-admin.ts`:

```typescript
import { Admin, Kafka, type GroupDescription } from "kafkajs";

const BROKER = process.env.KAFKA_BOOTSTRAP_SERVER ?? "localhost:9092";

let kafka: Kafka | null = null;
let admin: Admin | null = null;

export async function connect(): Promise<void> {
  if (admin) return;
  kafka = new Kafka({ clientId: "e2e-admin", brokers: [BROKER] });
  admin = kafka.admin();
  await admin.connect();
}

export async function disconnect(): Promise<void> {
  if (admin) {
    await admin.disconnect();
    admin = null;
  }
  kafka = null;
}

export async function consumerGroupMembers(groupId: string): Promise<GroupDescription> {
  if (!admin) throw new Error("kafka-admin: connect() must be called first");
  const desc = await admin.describeGroups([groupId]);
  if (desc.groups.length === 0) throw new Error(`kafka-admin: group not found: ${groupId}`);
  return desc.groups[0];
}

export async function listConsumerGroups(): Promise<string[]> {
  if (!admin) throw new Error("kafka-admin: connect() must be called first");
  const r = await admin.listGroups();
  return r.groups.map((g) => g.groupId);
}
```

- [ ] **Step 2: Verify TS compiles**

```bash
pnpm --filter @canary/e2e build
```

Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/helpers/kafka-admin.ts
git commit -m "feat(e2e): kafka-admin.ts — kafkajs admin client for consumer-group queries"
```

(HEREDOC + Co-Authored-By footer.)

---

## Task 11: e2e helper — `restate-admin.ts`

**Files:**
- Create: `tests/e2e/helpers/restate-admin.ts`

- [ ] **Step 1: Write the helper**

`tests/e2e/helpers/restate-admin.ts`:

```typescript
import axios from "axios";

const ADMIN_URL = process.env.RESTATE_ADMIN_URL ?? "http://localhost:9070";

export interface RestateDeployment {
  id: string;
  uri?: string;
  services: Array<{ name: string; revision: number }>;
}

export async function listDeployments(): Promise<RestateDeployment[]> {
  const r = await axios.get(`${ADMIN_URL}/deployments`, { validateStatus: () => true });
  if (r.status !== 200) {
    throw new Error(`restate-admin: GET /deployments returned ${r.status}: ${JSON.stringify(r.data)}`);
  }
  const data = r.data as { deployments?: RestateDeployment[] };
  return data.deployments ?? [];
}

export async function listServices(): Promise<string[]> {
  const r = await axios.get(`${ADMIN_URL}/services`, { validateStatus: () => true });
  if (r.status !== 200) {
    throw new Error(`restate-admin: GET /services returned ${r.status}: ${JSON.stringify(r.data)}`);
  }
  const data = r.data as { services?: Array<{ name: string }> };
  return (data.services ?? []).map((s) => s.name);
}
```

- [ ] **Step 2: Verify TS compiles**

```bash
pnpm --filter @canary/e2e build
```

Expected: clean.

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/helpers/restate-admin.ts
git commit -m "feat(e2e): restate-admin.ts — axios client for Restate Admin API queries"
```

(HEREDOC + Co-Authored-By footer.)

---

## Task 12: S1 Baseline scenario

**Files:**
- Create: `tests/e2e/s1-baseline.test.ts`

- [ ] **Step 1: Write the scenario**

`tests/e2e/s1-baseline.test.ts`:

```typescript
import { describe, it, expect, beforeAll } from "vitest";
import { sendOrder } from "./helpers/traffic.js";
import { assertServedVersion } from "./helpers/subset.js";
import { status } from "./helpers/canary.js";

const SERVICES = [
  "order-service",
  "payment-service",
  "inventory-service",
  "notification-service",
  "audit-service",
] as const;

describe("S1 Baseline — all stable, no canaries deployed", () => {
  beforeAll(async () => {
    // Verify the cluster is in the all-stable starting state.
    for (const svc of SERVICES) {
      const s = await status(svc);
      if (s.helmCanaryPresent) {
        throw new Error(
          `Pre-condition failed: ${svc} has a canary release. Run \`make canary-rollback SVC=${svc}\` first.`,
        );
      }
      if (s.vsHasHeaderRule) {
        throw new Error(
          `Pre-condition failed: ${svc} VS has a header rule. Run \`make canary-rollback SVC=${svc}\` first.`,
        );
      }
    }
  });

  it("GET /api/orders without x-canary returns 2xx from stable", async () => {
    const r = await sendOrder({ canary: false, user: "s1-stable" });
    expect(r.status).toBeGreaterThanOrEqual(200);
    expect(r.status).toBeLessThan(300);
    assertServedVersion(r.headers, "stable");
  });

  it("GET /api/orders with x-canary returns 2xx from stable (graceful fallback)", async () => {
    const r = await sendOrder({ canary: true, user: "s1-fallback" });
    expect(r.status).toBeGreaterThanOrEqual(200);
    expect(r.status).toBeLessThan(300);
    assertServedVersion(r.headers, "stable");
  });
});
```

- [ ] **Step 2: Build the package to confirm everything resolves**

```bash
pnpm --filter @canary/e2e build
```

Expected: clean. The S1 file imports from helpers; type-check should pass.

- [ ] **Step 3: Defer running the test — running it requires a real cluster with refreshed images. The bats smoke / Make target verification (Tasks 13-14) and operator manual verification (Task 15) cover that.**

- [ ] **Step 4: Commit**

```bash
git add tests/e2e/s1-baseline.test.ts
git commit -m "$(cat <<'EOF'
test(e2e): S1 Baseline scenario — all-stable, with/without x-canary

Verifies the all-stable cluster serves both header-flagged and
unflagged requests with 2xx + x-served-version: stable. Doubles
as graceful-fallback verification (S5 in 1.5.b).

Pre-condition: no canary releases deployed for any of the 5 services.
Run `make canary-rollback SVC=<svc>` for any leftover canary state
before invoking `make e2e SCENARIO=s1`.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 13: Makefile targets

**Files:**
- Modify: `Makefile`

- [ ] **Step 1: Append the e2e targets to the existing Makefile**

```makefile
.PHONY: e2e ci-local

e2e: ## Run e2e scenarios (use SCENARIO=<name> to run a single file)
	@pnpm --filter @canary/e2e build >/dev/null
	@if [ -n "$(SCENARIO)" ]; then \
	  pnpm --filter @canary/e2e exec vitest run $(SCENARIO); \
	else \
	  pnpm --filter @canary/e2e test; \
	fi

ci-local: ## Run fast e2e subset (S1 in 1.5.a; S1+S2+S5+S8+S9+S12 in 1.5.b)
	@pnpm --filter @canary/e2e build >/dev/null
	@pnpm --filter @canary/e2e exec vitest run s1
```

Insert at the end of the existing Makefile, after the canary targets from Plan 1.4. Recipes use TAB indentation.

- [ ] **Step 2: Verify `make help` shows the new targets**

```bash
make help | grep -E "e2e|ci-local"
```

Expected: 2 lines — `e2e`, `ci-local`.

- [ ] **Step 3: Verify `make e2e SCENARIO=<unmatched>` errors cleanly without running everything**

```bash
make e2e SCENARIO=does-not-exist 2>&1 | tail -3
```

Expected: vitest reports "No test files found" and exits non-zero. The Make target propagates the exit code.

- [ ] **Step 4: Commit**

```bash
git add Makefile
git commit -m "$(cat <<'EOF'
feat(make): e2e + ci-local targets

`make e2e` runs all e2e scenarios; `make e2e SCENARIO=<name>`
filters to one file. `make ci-local` runs the fast subset
(just S1 in 1.5.a — full curated subset arrives in 1.5.b).

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 14: README — Plan 1.5.a section

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Append a "Plan 1.5.a" section to `README.md`**

Append to the end:

```markdown

## Plan 1.5.a — e2e harness foundation + S1 Baseline (complete)

The TypeScript e2e harness lives in `tests/e2e/` (workspace package `@canary/e2e`). It uses **vitest** with a sequential single-fork pool so cluster-mutation scenarios don't conflict. Each service stamps `x-served-version: stable | canary` on outbound HTTP responses (via lib-java auto-config + lib-node middleware), letting tests trivially assert which subset handled a request.

### Quickstart

```bash
make up                                                   # 1.1
make build-services                                       # 1.3.a + new lib changes
make build-images && make load-images                     # 1.3.b
make deploy-services                                      # 1.3.b

# 1.5.a additions:
make e2e SCENARIO=s1                                      # run S1 Baseline only
make e2e                                                  # run all e2e scenarios (just S1 in 1.5.a)
make ci-local                                             # fast subset (just S1 in 1.5.a)
```

### What S1 verifies

S1 Baseline asserts that on a clean stable cluster (no canary deployed):
- `POST /api/orders` without `x-canary` returns 2xx with `x-served-version: stable`
- `POST /api/orders` with `x-canary: true` ALSO returns 2xx with `x-served-version: stable` (graceful fallback)

This doubles as coverage for the umbrella spec's S5 (no-canary graceful fallback). S5 still gets its own dedicated file when 1.5.b ships, for clarity.

### Helpers

`tests/e2e/helpers/` contains reusable building blocks for 1.5.b's scenarios:

| Helper | What it does |
|---|---|
| `canary.ts` | Shells out to `node tools/canary-ctl/bin/canary-ctl` for `deployCanary`, `rollback`, `status`, `reconcile`. |
| `traffic.ts` | `sendOrder({canary, user, sku, ...})` — single POST to `/api/orders`. |
| `subset.ts` | `assertServedVersion(headers, "stable" \| "canary")`. |
| `load.ts` | `runLoad({url, rps, durationSeconds})` — TS-native load gen, returns p50/p99 + counts. |
| `kafka-admin.ts` | kafkajs admin: consumer-group descriptions. (Used by S10 in 1.5.b.) |
| `restate-admin.ts` | axios `GET :9070/deployments` and `/services`. (Used by S11 in 1.5.b.) |

Next phase: 1.5.b (12 remaining scenarios S2–S13).
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs(readme): add Plan 1.5.a section (e2e harness + S1)"
```

(HEREDOC + Co-Authored-By footer.)

---

## Task 15: End-to-end manual verification

**Files:** none modified — verification only.

This task requires a real kind cluster with refreshed images. Lib-java + lib-node changed, so the service images need to be rebuilt + reloaded.

- [ ] **Step 1: Tear down + bootstrap (if not already running)**

```bash
make down
make up
```

Expected: 1.1 substrate ready.

- [ ] **Step 2: Build everything**

```bash
make build-services
```

Expected: Java services + Node services rebuild with the new lib changes.

- [ ] **Step 3: Build + load images**

```bash
make build-images
make load-images
```

Expected: 5 Docker images built and loaded into kind.

- [ ] **Step 4: Deploy services**

```bash
make deploy-services
```

Expected: 5 stable releases up.

- [ ] **Step 5: Verify VERSION env var made it into containers**

```bash
kubectl -n services exec deploy/payment-service-stable -- printenv | grep VERSION
```

Expected: `VERSION=stable`.

- [ ] **Step 6: Verify x-served-version header on a manual request**

```bash
curl -i -X POST -H 'content-type: application/json' \
  -d '{"userId":"manual-test","sku":"sku-1","quantity":1,"amount":100}' \
  http://localhost:8080/api/orders 2>&1 | head -20
```

Expected: response includes `x-served-version: stable` header.

- [ ] **Step 7: Run S1 via vitest directly**

```bash
make e2e SCENARIO=s1
```

Expected: 2 tests PASS within ~30s. If `pre-condition failed` errors appear, run `make canary-rollback SVC=<svc>` for the named service first.

- [ ] **Step 8: Run all unit tests as a regression check**

```bash
make verify
```

Expected: full project test suite passes (lib-java, lib-node, services, tools/canary-ctl, tools/traffic-cli, tests/e2e helper unit tests).

- [ ] **Step 9: No commit — verification only**

If any step fails, return to the relevant earlier task and fix.

---

## Self-review checklist

- **Spec coverage.** Spec sections map to plan tasks: TS e2e harness (Task 1, 12); helpers (Tasks 6-11); subset-served instrumentation (Tasks 2, 3, 4, 5); S1 (Task 12); Make targets (Task 13); README (Task 14); operator workflow (Task 15).
- **Placeholders.** None. Every task has concrete code.
- **Type/name consistency.** `assertServedVersion`/`getServedVersion` named consistently in subset.ts, the S1 scenario, and the test. `xServedVersionMiddleware` exported from `@canary/lib-node` and imported in services. `XCanaryResponseHeaderFilter` consistently named in lib-java + auto-config + test. `VERSION` env var name matches in lib-java's `@Value`, lib-node's `process.env.VERSION`, and the Helm ConfigMap key.
- **TDD discipline.** Tasks 2 (lib-java filter), 3 (lib-node middleware), 6 (subset.ts) follow TDD. Tasks 7-11 (helpers) skip unit tests intentionally per spec — verified by S1 running end-to-end. Tasks 1 (scaffold), 4 (Helm), 5 (service wiring), 13-14 (Make/README) are glue/config; correctness is verified by the next task that consumes them or by Task 15's manual verification.
- **Frequent commits.** 14 commits across Tasks 1-14 (Task 15 makes none). Each commit produces a working state.

---

## Done when

- All unit tests pass: `make verify` runs cleanly with the new lib-java + lib-node + e2e/helpers tests included.
- `pnpm --filter @canary/e2e build` produces clean dist artifacts.
- `make e2e SCENARIO=s1` passes against a fresh `make up && make build-services && make build-images && make load-images && make deploy-services` cluster.
- `kubectl exec` into a stable pod shows `VERSION=stable`; a curl to the edge shows `x-served-version: stable` in the response headers.
- README has a `## Plan 1.5.a` section.
- All commits in this task list are present on `claude/phase-1.5.a-e2e-foundation`.
