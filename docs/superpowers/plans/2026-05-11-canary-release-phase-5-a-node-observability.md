# Canary Release Phase 5.a-node — Node Service Observability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring `services/order-service` and `services/notification-service` to observability parity with the Java services that landed in 5.a. Same metric names, same tag set, same lane semantics. Add a `platform/lib-node/src/observability/` package containing the TypeScript mirrors of the Java helpers, then wire both Node services to initialize OTel tracing, emit metrics, and expose `/actuator/prometheus`.

**Architecture:** New package `platform/lib-node/src/observability/` exporting `canaryLane`, `CanaryMetrics`, `canaryHttpMetricsMiddleware`, `wrapKafkaConsumer`, `measureRestate`, `LaneStateProbe`, `initTracing`, and `canaryMetricsEndpoint`. Metrics use `prom-client` (the de-facto Node Prometheus library). Tracing uses `@opentelemetry/sdk-node` + `@opentelemetry/auto-instrumentations-node` + `@opentelemetry/exporter-trace-otlp-grpc`. Per-service `tracing.ts` initializes the SDK as the FIRST import of each entry point. The Helm scrape annotation from 5.a Task 15 (`prometheus.io/path: "/actuator/prometheus"`) is reused — Node services expose the prom registry at the same path Java does.

**Tech Stack:** TypeScript / Node 20+, Express 4, KafkaJS 2.2, `@opentelemetry/api`, `@opentelemetry/sdk-node` (1.x line — match the SDK API surface), `@opentelemetry/auto-instrumentations-node`, `@opentelemetry/exporter-trace-otlp-grpc`, `prom-client`, vitest + supertest for tests, `@kubernetes/client-node` (already a dep).

---

## File Structure

### Created

| Path | Responsibility |
|---|---|
| `platform/lib-node/src/observability/canary-lane-tag.ts` | `currentLane(): "stable" \| "canary"` — reads `XCanaryContext`. |
| `platform/lib-node/src/observability/canary-metrics.ts` | `CanaryMetrics` class wrapping a `prom-client` `Registry`. Public API: `recordHttp/recordKafka/recordRestate(target, outcome, durationSeconds)` + `recordShadowMismatch(field)`. Constructor takes `(serviceName, registry?)`. |
| `platform/lib-node/src/observability/canary-http-metrics-middleware.ts` | `canaryHttpMetricsMiddleware(metrics)` returns an Express middleware that times the request, classifies outcome by `res.statusCode`, and calls `metrics.recordHttp`. |
| `platform/lib-node/src/observability/canary-kafka-metrics.ts` | `wrapKafkaConsumer(consumer, metrics)` wraps a `kafkajs` `Consumer` whose `run({ eachMessage })` callback is timed + tagged before delegating. |
| `platform/lib-node/src/observability/canary-restate-meter.ts` | `measureRestate<T>(handlerName, body: () => Promise<T>): Promise<T>` — times the body, classifies success/server_error, calls `metrics.recordRestate`. Used by 5.b to wrap handler bodies. |
| `platform/lib-node/src/observability/lane-state-probe.ts` | `LaneStateProbe` watches `<service>-stable` and `<service>-canary` Endpoints; updates the `canary_lane_active` gauge. Mirrors the existing `XCanaryPresenceWatcher` shape. |
| `platform/lib-node/src/observability/tracing.ts` | `initTracing(serviceName)` initializes `NodeSDK` with OTLP gRPC exporter and a custom `SpanProcessor` that adds `canary.lane` + `canary.service` attributes to spans at start. |
| `platform/lib-node/src/observability/canary-metrics-endpoint.ts` | `canaryMetricsEndpoint(metrics)` returns an Express request handler that responds with the prom registry's text-format output. |
| `platform/lib-node/src/__tests__/canary-lane-tag.test.ts` | Tests: returns `"stable"`/`"canary"` based on `XCanaryContext`. |
| `platform/lib-node/src/__tests__/canary-metrics.test.ts` | Tests against a fresh `prom-client.Registry`: each `recordX` produces a meter with the expected name + label set. |
| `platform/lib-node/src/__tests__/canary-http-metrics-middleware.test.ts` | Tests using supertest + an express app: requests increment `canary_request_total` with correct outcome (success / client_error / server_error). |
| `platform/lib-node/src/__tests__/canary-kafka-metrics.test.ts` | Tests: a mocked `kafkajs` consumer's `eachMessage` is wrapped; success path records `success` counter; thrown path records `server_error`. |
| `platform/lib-node/src/__tests__/canary-restate-meter.test.ts` | Tests: success path records `success`; throw records `server_error`; timer always records. |
| `platform/lib-node/src/__tests__/lane-state-probe.test.ts` | Tests using a mocked k8s API: `setLaneActive` toggles the gauge; gauge has the right labels. |
| `platform/lib-node/src/__tests__/tracing.test.ts` | Tests: `initTracing` returns a `NodeSDK` instance that has the OTLP exporter configured (we don't spin it up — just verify config). |
| `platform/lib-node/src/__tests__/canary-metrics-endpoint.test.ts` | Tests using supertest: GET on the handler returns 200 with `text/plain; version=0.0.4` content-type and the registry's metrics text. |
| `services/order-service/src/tracing.ts` | One-liner: `import { initTracing } from "@canary/lib-node"; initTracing("order");`. |
| `services/notification-service/src/tracing.ts` | One-liner: `import { initTracing } from "@canary/lib-node"; initTracing("notification");`. |

### Modified

| Path | Change |
|---|---|
| `platform/lib-node/package.json` | Add deps: `@opentelemetry/api`, `@opentelemetry/sdk-node`, `@opentelemetry/auto-instrumentations-node`, `@opentelemetry/exporter-trace-otlp-grpc`, `prom-client`. Add devDep `supertest`. |
| `platform/lib-node/src/index.ts` | Append `export * from "./observability/...";` lines for each new module. |
| `services/order-service/src/index.ts` | Add `import "./tracing.js";` as FIRST import; instantiate `CanaryMetrics`; register `canaryHttpMetricsMiddleware` + `canaryMetricsEndpoint`; wrap Kafka consumer; start + shutdown `LaneStateProbe`. |
| `services/notification-service/src/index.ts` | Same as above with `"notification"` service name. |
| `services/order-service/package.json` | No dep changes needed (already pulls `@canary/lib-node` workspace), but verify. |
| `services/notification-service/package.json` | Same — verify. |
| `pnpm-lock.yaml` | Regenerated by `pnpm install`. |

---

## Task 1 — Add OTel + prom-client deps to lib-node

**Files:**
- Modify: `platform/lib-node/package.json`

- [ ] **Step 1.1: Add dependencies**

Edit `platform/lib-node/package.json`. Inside `dependencies`, add the five new entries; inside `devDependencies`, add `supertest`. Final block:

```json
{
  "dependencies": {
    "@kubernetes/client-node": "^1.0.0",
    "@opentelemetry/api": "^1.9.0",
    "@opentelemetry/auto-instrumentations-node": "^0.50.0",
    "@opentelemetry/exporter-trace-otlp-grpc": "^0.55.0",
    "@opentelemetry/sdk-node": "^0.55.0",
    "axios": "^1.7.7",
    "express": "^4.21.0",
    "kafkajs": "^2.2.4",
    "prom-client": "^15.1.3",
    "@restatedev/restate-sdk": "^1.14.2"
  },
  "devDependencies": {
    "@types/express": "^5.0.0",
    "@types/node": "^22.7.4",
    "@types/supertest": "^6.0.2",
    "supertest": "^7.0.0",
    "typescript": "^5.6.2",
    "vitest": "^2.1.1"
  }
}
```

The OTel SDK version `0.55.x` and API version `1.9.x` are the line that pairs cleanly. **Verify latest stable** at task time via the same `repo1.maven.org`-style direct check used in 5.a's dep correction (for npm: `npm view @opentelemetry/sdk-node version` and `npm view @opentelemetry/auto-instrumentations-node version`); bump the floor accordingly.

- [ ] **Step 1.2: Install + verify**

Run from repo root:
```bash
pnpm install
```
Expected: succeeds without errors, `pnpm-lock.yaml` updated.

Run:
```bash
pnpm --filter @canary/lib-node test
```
Expected: existing lib-node tests still green. New deps must not regress existing tests.

- [ ] **Step 1.3: Commit**

```bash
git add platform/lib-node/package.json pnpm-lock.yaml
git commit -m "build(lib-node): add OTel SDK + prom-client + supertest deps"
```

---

## Task 2 — `currentLane()` helper

**Files:**
- Create: `platform/lib-node/src/observability/canary-lane-tag.ts`
- Create: `platform/lib-node/src/__tests__/canary-lane-tag.test.ts`

- [ ] **Step 2.1: Write the failing test**

Create `platform/lib-node/src/__tests__/canary-lane-tag.test.ts`:

```typescript
import { describe, expect, it } from "vitest";
import { runWithCanary } from "../x-canary-context.js";
import { currentLane } from "../observability/canary-lane-tag.js";

describe("currentLane", () => {
  it("returns 'stable' when no context", () => {
    expect(currentLane()).toBe("stable");
  });

  it("returns 'canary' inside a canary context", async () => {
    await runWithCanary(true, async () => {
      expect(currentLane()).toBe("canary");
    });
  });

  it("returns 'stable' inside a stable context", async () => {
    await runWithCanary(false, async () => {
      expect(currentLane()).toBe("stable");
    });
  });
});
```

- [ ] **Step 2.2: Run, verify FAIL**

Run: `pnpm --filter @canary/lib-node test -- canary-lane-tag`
Expected: FAIL — cannot resolve `../observability/canary-lane-tag.js`.

- [ ] **Step 2.3: Implement**

Create `platform/lib-node/src/observability/canary-lane-tag.ts`:

```typescript
import { isCanary } from "../x-canary-context.js";

export type Lane = "stable" | "canary";

export const STABLE: Lane = "stable";
export const CANARY: Lane = "canary";

/** Returns the current lane derived from XCanaryContext. */
export function currentLane(): Lane {
  return isCanary() ? CANARY : STABLE;
}
```

- [ ] **Step 2.4: Run, verify PASS**

Run: `pnpm --filter @canary/lib-node test -- canary-lane-tag`
Expected: 3 tests pass.

- [ ] **Step 2.5: Commit**

```bash
git add platform/lib-node/src/observability/canary-lane-tag.ts \
        platform/lib-node/src/__tests__/canary-lane-tag.test.ts
git commit -m "feat(observability): currentLane helper resolves lane string from XCanaryContext"
```

---

## Task 3 — `CanaryMetrics` central helper

**Files:**
- Create: `platform/lib-node/src/observability/canary-metrics.ts`
- Create: `platform/lib-node/src/__tests__/canary-metrics.test.ts`

- [ ] **Step 3.1: Write the failing test**

Create `platform/lib-node/src/__tests__/canary-metrics.test.ts`:

```typescript
import { Registry } from "prom-client";
import { describe, expect, it } from "vitest";
import { runWithCanary } from "../x-canary-context.js";
import { CanaryMetrics } from "../observability/canary-metrics.js";

describe("CanaryMetrics", () => {
  it("recordHttp increments counter with expected labels", async () => {
    const registry = new Registry();
    const metrics = new CanaryMetrics("payment", registry);

    await runWithCanary(false, async () => {
      metrics.recordHttp("POST /pay", "success", 0.042);
    });

    const c = await registry.getSingleMetric("canary_request_total")?.get();
    expect(c?.values).toContainEqual(
      expect.objectContaining({
        value: 1,
        labels: { substrate: "http", service: "payment", lane: "stable", outcome: "success", target: "POST /pay" },
      }),
    );
  });

  it("recordHttp records histogram with expected labels", async () => {
    const registry = new Registry();
    const metrics = new CanaryMetrics("payment", registry);

    await runWithCanary(true, async () => {
      metrics.recordHttp("GET /healthz", "success", 0.007);
    });

    const h = await registry.getSingleMetric("canary_request_duration_seconds")?.get();
    const sum = h?.values.find((v) =>
      v.metricName === "canary_request_duration_seconds_sum" &&
      v.labels.target === "GET /healthz" &&
      v.labels.lane === "canary",
    );
    expect(sum?.value).toBeCloseTo(0.007);
  });

  it("recordKafka uses kafka substrate and topic target", async () => {
    const registry = new Registry();
    const metrics = new CanaryMetrics("audit", registry);

    await runWithCanary(true, async () => {
      metrics.recordKafka("payments.charged", "success", 0.1);
    });

    const c = await registry.getSingleMetric("canary_request_total")?.get();
    expect(c?.values).toContainEqual(
      expect.objectContaining({
        value: 1,
        labels: { substrate: "kafka", service: "audit", lane: "canary", outcome: "success", target: "payments.charged" },
      }),
    );
  });

  it("recordRestate uses restate substrate and handler target", async () => {
    const registry = new Registry();
    const metrics = new CanaryMetrics("order", registry);

    metrics.recordRestate("CheckoutSagaStable.run", "server_error", 0.25);

    const c = await registry.getSingleMetric("canary_request_total")?.get();
    expect(c?.values).toContainEqual(
      expect.objectContaining({
        value: 1,
        labels: { substrate: "restate", service: "order", lane: "stable", outcome: "server_error", target: "CheckoutSagaStable.run" },
      }),
    );
  });

  it("recordShadowMismatch increments by service+field", async () => {
    const registry = new Registry();
    const metrics = new CanaryMetrics("payment", registry);

    metrics.recordShadowMismatch("totalCents");

    const c = await registry.getSingleMetric("canary_shadow_mismatch_total")?.get();
    expect(c?.values).toContainEqual(
      expect.objectContaining({
        value: 1,
        labels: { service: "payment", field: "totalCents" },
      }),
    );
  });
});
```

- [ ] **Step 3.2: Run, verify FAIL**

Run: `pnpm --filter @canary/lib-node test -- canary-metrics`
Expected: FAIL — `cannot resolve ../observability/canary-metrics.js`.

- [ ] **Step 3.3: Implement**

Create `platform/lib-node/src/observability/canary-metrics.ts`:

```typescript
import { Counter, Histogram, Registry, register as defaultRegister } from "prom-client";
import { currentLane } from "./canary-lane-tag.js";

export type Substrate = "http" | "kafka" | "restate";
export type Outcome = "success" | "client_error" | "server_error";

const COUNTER_NAME = "canary_request_total";
const HISTOGRAM_NAME = "canary_request_duration_seconds";
const SHADOW_NAME = "canary_shadow_mismatch_total";

export class CanaryMetrics {
  private readonly counter: Counter<string>;
  private readonly histogram: Histogram<string>;
  private readonly shadow: Counter<string>;

  constructor(
    private readonly serviceName: string,
    private readonly registry: Registry = defaultRegister,
  ) {
    this.counter = new Counter({
      name: COUNTER_NAME,
      help: "Total canary-tagged requests per substrate/service/lane.",
      labelNames: ["substrate", "service", "lane", "outcome", "target"],
      registers: [registry],
    });
    this.histogram = new Histogram({
      name: HISTOGRAM_NAME,
      help: "Canary-tagged request duration in seconds.",
      labelNames: ["substrate", "service", "lane", "target"],
      registers: [registry],
    });
    this.shadow = new Counter({
      name: SHADOW_NAME,
      help: "Canary shadow-read field mismatches.",
      labelNames: ["service", "field"],
      registers: [registry],
    });
  }

  recordHttp(target: string, outcome: Outcome, durationSeconds: number): void {
    this.record("http", target, outcome, durationSeconds);
  }

  recordKafka(target: string, outcome: Outcome, durationSeconds: number): void {
    this.record("kafka", target, outcome, durationSeconds);
  }

  recordRestate(target: string, outcome: Outcome, durationSeconds: number): void {
    this.record("restate", target, outcome, durationSeconds);
  }

  recordShadowMismatch(field: string): void {
    this.shadow.inc({ service: this.serviceName, field });
  }

  /** For service wiring: hand the registry to the metrics endpoint. */
  getRegistry(): Registry {
    return this.registry;
  }

  private record(substrate: Substrate, target: string, outcome: Outcome, durationSeconds: number): void {
    const lane = currentLane();
    this.counter.inc({
      substrate,
      service: this.serviceName,
      lane,
      outcome,
      target,
    });
    this.histogram.observe(
      { substrate, service: this.serviceName, lane, target },
      durationSeconds,
    );
  }
}
```

- [ ] **Step 3.4: Run, verify PASS**

Run: `pnpm --filter @canary/lib-node test -- canary-metrics`
Expected: 5 tests pass.

- [ ] **Step 3.5: Commit**

```bash
git add platform/lib-node/src/observability/canary-metrics.ts \
        platform/lib-node/src/__tests__/canary-metrics.test.ts
git commit -m "feat(observability): CanaryMetrics central helper for the four canary-aware meters (Node)"
```

---

## Task 4 — `canaryHttpMetricsMiddleware`

**Files:**
- Create: `platform/lib-node/src/observability/canary-http-metrics-middleware.ts`
- Create: `platform/lib-node/src/__tests__/canary-http-metrics-middleware.test.ts`

- [ ] **Step 4.1: Write the failing test**

Create `platform/lib-node/src/__tests__/canary-http-metrics-middleware.test.ts`:

```typescript
import express from "express";
import { Registry } from "prom-client";
import request from "supertest";
import { describe, expect, it } from "vitest";
import { X_CANARY_HEADER, X_CANARY_TRUE } from "../x-canary-constants.js";
import { xCanaryMiddleware } from "../x-canary-middleware.js";
import { canaryHttpMetricsMiddleware } from "../observability/canary-http-metrics-middleware.js";
import { CanaryMetrics } from "../observability/canary-metrics.js";

function appWith(metrics: CanaryMetrics) {
  const app = express();
  app.use(xCanaryMiddleware);
  app.use(canaryHttpMetricsMiddleware(metrics));
  app.get("/healthz", (_req, res) => res.status(200).send("ok"));
  app.post("/pay", (_req, res) => res.status(404).send("missing"));
  app.get("/boom", (_req, res) => res.status(503).send("down"));
  return app;
}

describe("canaryHttpMetricsMiddleware", () => {
  it("records success outcome on 2xx canary request", async () => {
    const registry = new Registry();
    const metrics = new CanaryMetrics("payment", registry);
    await request(appWith(metrics))
      .get("/healthz")
      .set(X_CANARY_HEADER, X_CANARY_TRUE);

    const c = await registry.getSingleMetric("canary_request_total")?.get();
    expect(c?.values).toContainEqual(
      expect.objectContaining({
        value: 1,
        labels: expect.objectContaining({
          outcome: "success",
          lane: "canary",
          target: "GET /healthz",
        }),
      }),
    );
  });

  it("records client_error on 4xx", async () => {
    const registry = new Registry();
    const metrics = new CanaryMetrics("payment", registry);
    await request(appWith(metrics)).post("/pay");

    const c = await registry.getSingleMetric("canary_request_total")?.get();
    expect(c?.values).toContainEqual(
      expect.objectContaining({
        value: 1,
        labels: expect.objectContaining({ outcome: "client_error" }),
      }),
    );
  });

  it("records server_error on 5xx", async () => {
    const registry = new Registry();
    const metrics = new CanaryMetrics("payment", registry);
    await request(appWith(metrics)).get("/boom");

    const c = await registry.getSingleMetric("canary_request_total")?.get();
    expect(c?.values).toContainEqual(
      expect.objectContaining({
        value: 1,
        labels: expect.objectContaining({ outcome: "server_error" }),
      }),
    );
  });
});
```

- [ ] **Step 4.2: Run, verify FAIL**

Run: `pnpm --filter @canary/lib-node test -- canary-http-metrics-middleware`
Expected: FAIL — module not found.

- [ ] **Step 4.3: Implement**

Create `platform/lib-node/src/observability/canary-http-metrics-middleware.ts`:

```typescript
import type { NextFunction, Request, Response } from "express";
import type { CanaryMetrics, Outcome } from "./canary-metrics.js";

/**
 * Express middleware that records canary HTTP metrics on response finish.
 * Must be registered AFTER xCanaryMiddleware so XCanaryContext is populated.
 */
export function canaryHttpMetricsMiddleware(metrics: CanaryMetrics) {
  return function metricsMiddleware(req: Request, res: Response, next: NextFunction): void {
    const startNs = process.hrtime.bigint();

    res.on("finish", () => {
      const elapsedSeconds = Number(process.hrtime.bigint() - startNs) / 1e9;
      const outcome = classifyStatus(res.statusCode);
      const target = `${req.method} ${routeOf(req)}`;
      metrics.recordHttp(target, outcome, elapsedSeconds);
    });

    next();
  };
}

function classifyStatus(status: number): Outcome {
  if (status >= 500) return "server_error";
  if (status >= 400) return "client_error";
  return "success";
}

function routeOf(req: Request): string {
  // Prefer matched route pattern (low cardinality) over raw URL.
  // After routing, Express sets req.route. Before routing — fall back to path.
  // Cast through any to bypass narrow Express type.
  const route = (req as unknown as { route?: { path?: string } }).route;
  return route?.path ?? req.path;
}
```

- [ ] **Step 4.4: Run, verify PASS**

Run: `pnpm --filter @canary/lib-node test -- canary-http-metrics-middleware`
Expected: 3 tests pass.

- [ ] **Step 4.5: Commit**

```bash
git add platform/lib-node/src/observability/canary-http-metrics-middleware.ts \
        platform/lib-node/src/__tests__/canary-http-metrics-middleware.test.ts
git commit -m "feat(observability): canaryHttpMetricsMiddleware times Express requests per-lane"
```

---

## Task 5 — `wrapKafkaConsumer`

**Files:**
- Create: `platform/lib-node/src/observability/canary-kafka-metrics.ts`
- Create: `platform/lib-node/src/__tests__/canary-kafka-metrics.test.ts`

- [ ] **Step 5.1: Write the failing test**

Create `platform/lib-node/src/__tests__/canary-kafka-metrics.test.ts`:

```typescript
import type { Consumer, EachMessagePayload, KafkaMessage } from "kafkajs";
import { Registry } from "prom-client";
import { describe, expect, it, vi } from "vitest";
import { CanaryMetrics } from "../observability/canary-metrics.js";
import { wrapKafkaConsumer } from "../observability/canary-kafka-metrics.js";

function fakeMessage(): KafkaMessage {
  return {
    key: Buffer.from("k"),
    value: Buffer.from("v"),
    timestamp: "0",
    size: 1,
    attributes: 0,
    offset: "0",
  };
}

function fakePayload(topic: string): EachMessagePayload {
  return {
    topic,
    partition: 0,
    message: fakeMessage(),
    heartbeat: async () => {},
    pause: () => () => {},
  };
}

describe("wrapKafkaConsumer", () => {
  it("records success when eachMessage resolves", async () => {
    const registry = new Registry();
    const metrics = new CanaryMetrics("audit", registry);
    const eachMessage = vi.fn().mockResolvedValue(undefined);

    let captured: ((p: EachMessagePayload) => Promise<void>) | undefined;
    const consumer = {
      run: vi.fn(async (cfg: { eachMessage?: (p: EachMessagePayload) => Promise<void> }) => {
        captured = cfg.eachMessage;
      }),
    } as unknown as Consumer;

    const wrapped = wrapKafkaConsumer(consumer, metrics);
    await wrapped.run({ eachMessage });

    await captured!(fakePayload("payments.charged"));
    expect(eachMessage).toHaveBeenCalled();

    const c = await registry.getSingleMetric("canary_request_total")?.get();
    expect(c?.values).toContainEqual(
      expect.objectContaining({
        value: 1,
        labels: expect.objectContaining({
          substrate: "kafka",
          outcome: "success",
          target: "payments.charged",
        }),
      }),
    );
  });

  it("records server_error and re-throws when eachMessage throws", async () => {
    const registry = new Registry();
    const metrics = new CanaryMetrics("audit", registry);
    const eachMessage = vi.fn().mockRejectedValue(new Error("boom"));

    let captured: ((p: EachMessagePayload) => Promise<void>) | undefined;
    const consumer = {
      run: vi.fn(async (cfg: { eachMessage?: (p: EachMessagePayload) => Promise<void> }) => {
        captured = cfg.eachMessage;
      }),
    } as unknown as Consumer;

    const wrapped = wrapKafkaConsumer(consumer, metrics);
    await wrapped.run({ eachMessage });

    await expect(captured!(fakePayload("payments.charged"))).rejects.toThrow("boom");

    const c = await registry.getSingleMetric("canary_request_total")?.get();
    expect(c?.values).toContainEqual(
      expect.objectContaining({
        value: 1,
        labels: expect.objectContaining({ outcome: "server_error" }),
      }),
    );
  });
});
```

- [ ] **Step 5.2: Run, verify FAIL**

Run: `pnpm --filter @canary/lib-node test -- canary-kafka-metrics`
Expected: FAIL.

- [ ] **Step 5.3: Implement**

Create `platform/lib-node/src/observability/canary-kafka-metrics.ts`:

```typescript
import type { Consumer, ConsumerRunConfig, EachMessagePayload } from "kafkajs";
import type { CanaryMetrics } from "./canary-metrics.js";

/**
 * Returns a wrapped Consumer whose `run({ eachMessage })` callback is timed
 * and recorded into CanaryMetrics. Other Consumer methods pass through.
 */
export function wrapKafkaConsumer(consumer: Consumer, metrics: CanaryMetrics): Consumer {
  const originalRun = consumer.run.bind(consumer);
  consumer.run = async function (config?: ConsumerRunConfig): Promise<void> {
    if (!config?.eachMessage) {
      return originalRun(config);
    }
    const userEachMessage = config.eachMessage;
    const wrapped: ConsumerRunConfig = {
      ...config,
      eachMessage: async (payload: EachMessagePayload) => {
        const startNs = process.hrtime.bigint();
        try {
          await userEachMessage(payload);
          const elapsed = Number(process.hrtime.bigint() - startNs) / 1e9;
          metrics.recordKafka(payload.topic, "success", elapsed);
        } catch (err) {
          const elapsed = Number(process.hrtime.bigint() - startNs) / 1e9;
          metrics.recordKafka(payload.topic, "server_error", elapsed);
          throw err;
        }
      },
    };
    return originalRun(wrapped);
  };
  return consumer;
}
```

- [ ] **Step 5.4: Run, verify PASS**

Run: `pnpm --filter @canary/lib-node test -- canary-kafka-metrics`
Expected: 2 tests pass.

- [ ] **Step 5.5: Commit**

```bash
git add platform/lib-node/src/observability/canary-kafka-metrics.ts \
        platform/lib-node/src/__tests__/canary-kafka-metrics.test.ts
git commit -m "feat(observability): wrapKafkaConsumer times Kafka consumer eachMessage per-lane"
```

---

## Task 6 — `measureRestate` helper

**Files:**
- Create: `platform/lib-node/src/observability/canary-restate-meter.ts`
- Create: `platform/lib-node/src/__tests__/canary-restate-meter.test.ts`

- [ ] **Step 6.1: Write the failing test**

Create `platform/lib-node/src/__tests__/canary-restate-meter.test.ts`:

```typescript
import { Registry } from "prom-client";
import { describe, expect, it } from "vitest";
import { CanaryMetrics } from "../observability/canary-metrics.js";
import { measureRestate } from "../observability/canary-restate-meter.js";

describe("measureRestate", () => {
  it("returns value and records success", async () => {
    const registry = new Registry();
    const metrics = new CanaryMetrics("order", registry);

    const result = await measureRestate(metrics, "CheckoutSagaStable.run", async () => "ok");

    expect(result).toBe("ok");
    const c = await registry.getSingleMetric("canary_request_total")?.get();
    expect(c?.values).toContainEqual(
      expect.objectContaining({
        value: 1,
        labels: expect.objectContaining({
          substrate: "restate",
          outcome: "success",
          target: "CheckoutSagaStable.run",
        }),
      }),
    );
  });

  it("records server_error when body throws and re-throws", async () => {
    const registry = new Registry();
    const metrics = new CanaryMetrics("order", registry);

    await expect(
      measureRestate(metrics, "CheckoutSagaStable.run", async () => {
        throw new Error("boom");
      }),
    ).rejects.toThrow("boom");

    const c = await registry.getSingleMetric("canary_request_total")?.get();
    expect(c?.values).toContainEqual(
      expect.objectContaining({
        value: 1,
        labels: expect.objectContaining({ outcome: "server_error" }),
      }),
    );
  });

  it("always records histogram", async () => {
    const registry = new Registry();
    const metrics = new CanaryMetrics("order", registry);

    await measureRestate(metrics, "PaymentVOStable.charge", async () => 1);

    const h = await registry.getSingleMetric("canary_request_duration_seconds")?.get();
    expect(h?.values.some((v) => v.metricName === "canary_request_duration_seconds_count")).toBe(true);
  });
});
```

- [ ] **Step 6.2: Run, verify FAIL**

Run: `pnpm --filter @canary/lib-node test -- canary-restate-meter`
Expected: FAIL.

- [ ] **Step 6.3: Implement**

Create `platform/lib-node/src/observability/canary-restate-meter.ts`:

```typescript
import type { CanaryMetrics } from "./canary-metrics.js";

/**
 * Wraps a Restate handler body with timer + outcome counter emission.
 * Records `success` if the body resolves, `server_error` if it throws.
 * Re-throws on error; non-Error throws are also classified server_error.
 */
export async function measureRestate<T>(
  metrics: CanaryMetrics,
  handlerName: string,
  body: () => Promise<T>,
): Promise<T> {
  const startNs = process.hrtime.bigint();
  try {
    const result = await body();
    const elapsed = Number(process.hrtime.bigint() - startNs) / 1e9;
    metrics.recordRestate(handlerName, "success", elapsed);
    return result;
  } catch (err) {
    const elapsed = Number(process.hrtime.bigint() - startNs) / 1e9;
    metrics.recordRestate(handlerName, "server_error", elapsed);
    throw err;
  }
}
```

- [ ] **Step 6.4: Run, verify PASS**

Run: `pnpm --filter @canary/lib-node test -- canary-restate-meter`
Expected: 3 tests pass.

- [ ] **Step 6.5: Commit**

```bash
git add platform/lib-node/src/observability/canary-restate-meter.ts \
        platform/lib-node/src/__tests__/canary-restate-meter.test.ts
git commit -m "feat(observability): measureRestate helper for handler-level metric emission (Node)"
```

---

## Task 7 — `LaneStateProbe`

**Files:**
- Create: `platform/lib-node/src/observability/lane-state-probe.ts`
- Create: `platform/lib-node/src/__tests__/lane-state-probe.test.ts`

The probe watches `Endpoints` named `<service>-stable` and `<service>-canary`. Mirrors `XCanaryPresenceWatcher`'s use of `@kubernetes/client-node` `Watch`.

- [ ] **Step 7.1: Write the failing test**

Create `platform/lib-node/src/__tests__/lane-state-probe.test.ts`:

```typescript
import type { V1Endpoints } from "@kubernetes/client-node";
import { Registry } from "prom-client";
import { describe, expect, it } from "vitest";
import { LaneStateProbe } from "../observability/lane-state-probe.js";

describe("LaneStateProbe", () => {
  it("registers gauges for both lanes at zero before any update", () => {
    const registry = new Registry();
    const probe = new LaneStateProbe("services", "payment", registry);

    probe.registerGauges();

    expect(probe.laneValue("stable")).toBe(0);
    expect(probe.laneValue("canary")).toBe(0);
  });

  it("setLaneActive toggles gauge value", () => {
    const registry = new Registry();
    const probe = new LaneStateProbe("services", "payment", registry);
    probe.registerGauges();

    probe.setLaneActive("canary", true);
    expect(probe.laneValue("canary")).toBe(1);

    probe.setLaneActive("canary", false);
    expect(probe.laneValue("canary")).toBe(0);
  });

  it("hasAddresses detects populated/empty Endpoints", () => {
    const populated: V1Endpoints = { subsets: [{ addresses: [{ ip: "10.0.0.1" }] }] };
    const empty: V1Endpoints = { subsets: [] };
    expect(LaneStateProbe.hasAddresses(populated)).toBe(true);
    expect(LaneStateProbe.hasAddresses(empty)).toBe(false);
    expect(LaneStateProbe.hasAddresses(undefined)).toBe(false);
  });

  it("gauge has substrate=http, service, lane labels", async () => {
    const registry = new Registry();
    const probe = new LaneStateProbe("services", "payment", registry);
    probe.registerGauges();
    probe.setLaneActive("stable", true);

    const g = await registry.getSingleMetric("canary_lane_active")?.get();
    expect(g?.values).toContainEqual(
      expect.objectContaining({
        value: 1,
        labels: { substrate: "http", service: "payment", lane: "stable" },
      }),
    );
  });
});
```

- [ ] **Step 7.2: Run, verify FAIL**

Run: `pnpm --filter @canary/lib-node test -- lane-state-probe`
Expected: FAIL.

- [ ] **Step 7.3: Implement**

Create `platform/lib-node/src/observability/lane-state-probe.ts`:

```typescript
import { CoreV1Api, KubeConfig, type V1Endpoints, Watch } from "@kubernetes/client-node";
import { Gauge, type Registry, register as defaultRegister } from "prom-client";

const GAUGE_NAME = "canary_lane_active";

export class LaneStateProbe {
  private gauge?: Gauge<string>;
  private laneActive = new Map<string, number>([
    ["stable", 0],
    ["canary", 0],
  ]);
  private watchAbortControllers: AbortController[] = [];
  private closed = false;

  constructor(
    private readonly namespace: string,
    private readonly serviceName: string,
    private readonly registry: Registry = defaultRegister,
    private readonly kc: KubeConfig = LaneStateProbe.defaultKubeConfig(),
  ) {}

  registerGauges(): void {
    this.gauge = new Gauge({
      name: GAUGE_NAME,
      help: "1 when the lane has at least one ready endpoint, else 0.",
      labelNames: ["substrate", "service", "lane"],
      registers: [this.registry],
      collect: () => {
        if (!this.gauge) return;
        for (const [lane, value] of this.laneActive) {
          this.gauge.set({ substrate: "http", service: this.serviceName, lane }, value);
        }
      },
    });
    // Force an initial collect so the labels appear immediately.
    this.gauge.set({ substrate: "http", service: this.serviceName, lane: "stable" }, 0);
    this.gauge.set({ substrate: "http", service: this.serviceName, lane: "canary" }, 0);
  }

  setLaneActive(lane: "stable" | "canary", active: boolean): void {
    this.laneActive.set(lane, active ? 1 : 0);
    if (this.gauge) {
      this.gauge.set({ substrate: "http", service: this.serviceName, lane }, active ? 1 : 0);
    }
  }

  laneValue(lane: "stable" | "canary"): number {
    return this.laneActive.get(lane) ?? 0;
  }

  static hasAddresses(e: V1Endpoints | undefined): boolean {
    if (!e?.subsets) return false;
    return e.subsets.some((s) => Array.isArray(s.addresses) && s.addresses.length > 0);
  }

  async start(): Promise<void> {
    if (!this.gauge) this.registerGauges();
    await this.watchEndpoints(`${this.serviceName}-stable`, "stable");
    await this.watchEndpoints(`${this.serviceName}-canary`, "canary");
  }

  private async watchEndpoints(endpointsName: string, lane: "stable" | "canary"): Promise<void> {
    const coreApi = this.kc.makeApiClient(CoreV1Api);
    // Initial state
    try {
      const ep = await coreApi.readNamespacedEndpoints({ namespace: this.namespace, name: endpointsName });
      this.setLaneActive(lane, LaneStateProbe.hasAddresses(ep));
    } catch {
      this.setLaneActive(lane, false);
    }
    const watch = new Watch(this.kc);
    const ctrl = await watch.watch(
      `/api/v1/namespaces/${this.namespace}/endpoints`,
      { fieldSelector: `metadata.name=${endpointsName}` },
      (type: string, obj: V1Endpoints) => {
        const active = type !== "DELETED" && LaneStateProbe.hasAddresses(obj);
        this.setLaneActive(lane, active);
      },
      (_err) => {
        if (!this.closed) {
          // Best-effort reconnect after 1s
          setTimeout(() => { void this.watchEndpoints(endpointsName, lane); }, 1000);
        }
      },
    );
    this.watchAbortControllers.push(ctrl);
  }

  close(): void {
    this.closed = true;
    for (const c of this.watchAbortControllers) {
      try { c.abort(); } catch { /* ignore */ }
    }
  }

  private static defaultKubeConfig(): KubeConfig {
    const kc = new KubeConfig();
    try { kc.loadFromCluster(); } catch { kc.loadFromDefault(); }
    return kc;
  }
}
```

- [ ] **Step 7.4: Run, verify PASS**

Run: `pnpm --filter @canary/lib-node test -- lane-state-probe`
Expected: 4 tests pass.

- [ ] **Step 7.5: Commit**

```bash
git add platform/lib-node/src/observability/lane-state-probe.ts \
        platform/lib-node/src/__tests__/lane-state-probe.test.ts
git commit -m "feat(observability): LaneStateProbe emits canary_lane_active gauge (Node)"
```

---

## Task 8 — `initTracing`

**Files:**
- Create: `platform/lib-node/src/observability/tracing.ts`
- Create: `platform/lib-node/src/__tests__/tracing.test.ts`

- [ ] **Step 8.1: Write the failing test**

Create `platform/lib-node/src/__tests__/tracing.test.ts`:

```typescript
import { describe, expect, it } from "vitest";
import { buildTracingConfig } from "../observability/tracing.js";

describe("buildTracingConfig", () => {
  it("uses default OTLP endpoint when env not set", () => {
    const cfg = buildTracingConfig("order", {});
    expect(cfg.serviceName).toBe("order");
    expect(cfg.otlpEndpoint).toBe("http://jaeger-collector.istio-system:4317");
  });

  it("honors OTLP_TRACING_ENDPOINT env var", () => {
    const cfg = buildTracingConfig("notification", { OTLP_TRACING_ENDPOINT: "http://otel:4317" });
    expect(cfg.otlpEndpoint).toBe("http://otel:4317");
  });

  it("derives serviceName from arg, falls back to SERVICE_NAME env", () => {
    const cfg = buildTracingConfig(undefined, { SERVICE_NAME: "audit" });
    expect(cfg.serviceName).toBe("audit");
  });
});
```

- [ ] **Step 8.2: Run, verify FAIL**

Run: `pnpm --filter @canary/lib-node test -- tracing.test`
Expected: FAIL.

- [ ] **Step 8.3: Implement**

Create `platform/lib-node/src/observability/tracing.ts`:

```typescript
import type { Span, SpanProcessor } from "@opentelemetry/sdk-trace-node";
import { isCanary } from "../x-canary-context.js";

export interface TracingConfig {
  serviceName: string;
  otlpEndpoint: string;
}

const DEFAULT_OTLP = "http://jaeger-collector.istio-system:4317";

/** Pure-function config builder, exported for tests. */
export function buildTracingConfig(
  serviceName: string | undefined,
  env: Record<string, string | undefined>,
): TracingConfig {
  return {
    serviceName: serviceName ?? env.SERVICE_NAME ?? "unknown",
    otlpEndpoint: env.OTLP_TRACING_ENDPOINT ?? DEFAULT_OTLP,
  };
}

/**
 * Initializes the OpenTelemetry NodeSDK. MUST be called before any module
 * to be auto-instrumented is imported. Idempotent — subsequent calls are no-ops.
 *
 * Usage in a service entry point:
 *   import { initTracing } from "@canary/lib-node";
 *   initTracing("order");   // FIRST line; before any other import that uses HTTP/Kafka
 */
let started = false;
export function initTracing(serviceName?: string): void {
  if (started) return;
  started = true;
  // Lazy-load the SDK only when initTracing is actually called — keeps unit tests
  // (which import this module but never call initTracing) from pulling in the
  // heavy SDK dependency graph.
  const { NodeSDK } = require("@opentelemetry/sdk-node");
  const { getNodeAutoInstrumentations } = require("@opentelemetry/auto-instrumentations-node");
  const { OTLPTraceExporter } = require("@opentelemetry/exporter-trace-otlp-grpc");
  const cfg = buildTracingConfig(serviceName, process.env);

  const sdk = new NodeSDK({
    serviceName: cfg.serviceName,
    traceExporter: new OTLPTraceExporter({ url: cfg.otlpEndpoint }),
    instrumentations: [getNodeAutoInstrumentations()],
    spanProcessors: [new CanaryLaneSpanProcessor(cfg.serviceName)],
  });
  sdk.start();
  process.on("SIGTERM", () => { void sdk.shutdown(); });
}

class CanaryLaneSpanProcessor implements SpanProcessor {
  constructor(private readonly serviceName: string) {}
  onStart(span: Span): void {
    span.setAttribute("canary.lane", isCanary() ? "canary" : "stable");
    span.setAttribute("canary.service", this.serviceName);
  }
  onEnd(): void {}
  async forceFlush(): Promise<void> {}
  async shutdown(): Promise<void> {}
}
```

The `require()` calls inside `initTracing` keep the SDK out of the unit-test code path (the test imports `buildTracingConfig` only, never calls `initTracing`).

- [ ] **Step 8.4: Run, verify PASS**

Run: `pnpm --filter @canary/lib-node test -- tracing.test`
Expected: 3 tests pass.

- [ ] **Step 8.5: Commit**

```bash
git add platform/lib-node/src/observability/tracing.ts \
        platform/lib-node/src/__tests__/tracing.test.ts
git commit -m "feat(observability): initTracing — OTel NodeSDK + auto-instrumentations + canary.lane span attr"
```

---

## Task 9 — `canaryMetricsEndpoint`

**Files:**
- Create: `platform/lib-node/src/observability/canary-metrics-endpoint.ts`
- Create: `platform/lib-node/src/__tests__/canary-metrics-endpoint.test.ts`

- [ ] **Step 9.1: Write the failing test**

Create `platform/lib-node/src/__tests__/canary-metrics-endpoint.test.ts`:

```typescript
import express from "express";
import { Registry } from "prom-client";
import request from "supertest";
import { describe, expect, it } from "vitest";
import { CanaryMetrics } from "../observability/canary-metrics.js";
import { canaryMetricsEndpoint } from "../observability/canary-metrics-endpoint.js";

describe("canaryMetricsEndpoint", () => {
  it("returns prom text-format with canary metrics registered", async () => {
    const registry = new Registry();
    const metrics = new CanaryMetrics("payment", registry);
    metrics.recordHttp("GET /healthz", "success", 0.01);

    const app = express();
    app.get("/actuator/prometheus", canaryMetricsEndpoint(metrics));

    const res = await request(app).get("/actuator/prometheus");
    expect(res.status).toBe(200);
    expect(res.headers["content-type"]).toMatch(/text\/plain/);
    expect(res.text).toContain("canary_request_total");
    expect(res.text).toContain('service="payment"');
  });
});
```

- [ ] **Step 9.2: Run, verify FAIL**

Run: `pnpm --filter @canary/lib-node test -- canary-metrics-endpoint`
Expected: FAIL.

- [ ] **Step 9.3: Implement**

Create `platform/lib-node/src/observability/canary-metrics-endpoint.ts`:

```typescript
import type { Request, Response } from "express";
import type { CanaryMetrics } from "./canary-metrics.js";

/** Express handler exposing the prom-client registry's text-format output. */
export function canaryMetricsEndpoint(metrics: CanaryMetrics) {
  const registry = metrics.getRegistry();
  return async function (_req: Request, res: Response): Promise<void> {
    res.set("Content-Type", registry.contentType);
    res.status(200).send(await registry.metrics());
  };
}
```

- [ ] **Step 9.4: Run, verify PASS**

Run: `pnpm --filter @canary/lib-node test -- canary-metrics-endpoint`
Expected: 1 test passes.

- [ ] **Step 9.5: Commit**

```bash
git add platform/lib-node/src/observability/canary-metrics-endpoint.ts \
        platform/lib-node/src/__tests__/canary-metrics-endpoint.test.ts
git commit -m "feat(observability): canaryMetricsEndpoint exposes prom registry at /actuator/prometheus"
```

---

## Task 10 — Update `lib-node/index.ts` barrel

**Files:**
- Modify: `platform/lib-node/src/index.ts`

- [ ] **Step 10.1: Append the observability exports**

Append to `platform/lib-node/src/index.ts`:

```typescript
export * from "./observability/canary-lane-tag.js";
export * from "./observability/canary-metrics.js";
export * from "./observability/canary-http-metrics-middleware.js";
export * from "./observability/canary-kafka-metrics.js";
export * from "./observability/canary-restate-meter.js";
export * from "./observability/lane-state-probe.js";
export * from "./observability/canary-metrics-endpoint.js";
export * from "./observability/tracing.js";
```

- [ ] **Step 10.2: Verify build + tests**

Run: `pnpm --filter @canary/lib-node build && pnpm --filter @canary/lib-node test`
Expected: BUILD + all tests pass.

- [ ] **Step 10.3: Commit**

```bash
git add platform/lib-node/src/index.ts
git commit -m "feat(observability): export new observability surface from lib-node index"
```

---

## Task 11 — Wire `services/order-service`

**Files:**
- Create: `services/order-service/src/tracing.ts`
- Modify: `services/order-service/src/index.ts`

Read the existing `services/order-service/src/index.ts` before editing. The structure (axios setup → kafka setup → http setup → server.listen → restate setup → shutdown handler) must be preserved.

- [ ] **Step 11.1: Create per-service tracing.ts**

Create `services/order-service/src/tracing.ts`:

```typescript
import { initTracing } from "@canary/lib-node";
initTracing("order");
```

- [ ] **Step 11.2: Update index.ts**

Edit `services/order-service/src/index.ts` so the very FIRST import line is:

```typescript
import "./tracing.js";   // MUST be first — initializes OTel SDK before express/kafkajs load
```

Then in the same file, after the existing imports add:

```typescript
import {
  CanaryMetrics,
  canaryHttpMetricsMiddleware,
  canaryMetricsEndpoint,
  LaneStateProbe,
  wrapKafkaConsumer,
} from "@canary/lib-node";
```

After `const config = loadConfig();` add:

```typescript
const metrics = new CanaryMetrics("order");
const laneProbe = new LaneStateProbe(
  process.env.POD_NAMESPACE ?? "services",
  "order",
);
laneProbe.registerGauges();
void laneProbe.start();
```

Inside the `setupHttp` invocation context — i.e. after `const app = setupHttp({...})` — add:

```typescript
app.use(canaryHttpMetricsMiddleware(metrics));
app.get("/actuator/prometheus", canaryMetricsEndpoint(metrics));
```

In the existing `setupKafka` block, if a consumer is created in that function, you'll need to thread `metrics` into it and call `wrapKafkaConsumer(consumer, metrics)` at the point of `consumer.run(...)`. If the consumer creation is inside `setupKafka` and not exposed, add a `metrics?` field to `setupKafka`'s options interface and pass it through. Match the existing pattern.

In the existing `shutdown` handler, add:
```typescript
laneProbe.close();
```

- [ ] **Step 11.3: Build + test the service**

Run:
```bash
pnpm --filter @canary/order-service build
pnpm --filter @canary/order-service test
```
Expected: BUILD SUCCESSFUL, tests pass. Any pre-existing test that hits an Express route should still pass (the new middleware doesn't change behavior, only observes).

- [ ] **Step 11.4: Commit**

```bash
git add services/order-service/src/tracing.ts services/order-service/src/index.ts services/order-service/src/kafka.ts
git commit -m "feat(order-service): wire OTel tracing + canary metrics + lane gauge"
```

(`kafka.ts` is included only if you had to thread `metrics` into it.)

---

## Task 12 — Wire `services/notification-service`

**Files:**
- Create: `services/notification-service/src/tracing.ts`
- Modify: `services/notification-service/src/index.ts`

Apply the identical wiring as Task 11, with `serviceName: "notification"` everywhere. Use the same approach for kafka if `notification-service` has a consumer setup.

- [ ] **Step 12.1: Create tracing.ts**

Create `services/notification-service/src/tracing.ts`:

```typescript
import { initTracing } from "@canary/lib-node";
initTracing("notification");
```

- [ ] **Step 12.2: Update index.ts**

Same edits as Task 11.2, replacing `"order"` with `"notification"`.

- [ ] **Step 12.3: Build + test**

Run:
```bash
pnpm --filter @canary/notification-service build
pnpm --filter @canary/notification-service test
```
Expected: SUCCESS.

- [ ] **Step 12.4: Commit**

```bash
git add services/notification-service/src/tracing.ts services/notification-service/src/index.ts services/notification-service/src/kafka.ts
git commit -m "feat(notification-service): wire OTel tracing + canary metrics + lane gauge"
```

---

## Task 13 — End-to-end verification (deferred to user — needs cluster)

This task validates that the foundation works in a real cluster. Skipped during plan execution; ran by the user when convenient.

- [ ] **Step 13.1: Bring up the cluster**

`make all` per existing project convention. Honor `feedback_e2e_inpod_probes.md` — no `kubectl exec ... curl` against pods.

- [ ] **Step 13.2: Verify Prometheus scrapes Node services**

```bash
kubectl port-forward -n istio-system svc/prometheus 9090:9090 &
sleep 3
curl -s 'http://localhost:9090/api/v1/targets' | grep -E 'order-service|notification-service' | head -5
```
Expected: both Node services appear as scrape targets with `health: "up"`.

- [ ] **Step 13.3: Verify canary metrics show up**

Drive a request through the order-service (via Istio ingress port-forward). Then:
```bash
curl -s 'http://localhost:9090/api/v1/query?query=canary_request_total{service="order"}' | jq '.data.result | length'
```
Expected: ≥ 1.

- [ ] **Step 13.4: Verify Jaeger receives spans tagged with canary.lane**

```bash
kubectl port-forward -n istio-system svc/tracing 16686:80 &
curl -s 'http://localhost:16686/api/traces?service=order&tag=canary.lane%3Astable&limit=5' | jq '.data | length'
```
Expected: ≥ 1.

- [ ] **Step 13.5: Verify canary_lane_active gauge**

```bash
curl -s 'http://localhost:9090/api/v1/query?query=canary_lane_active{service="order"}' | jq '.data.result[]'
```
Expected: rows showing `lane=stable, value=1` and `lane=canary, value=0` (before deploying a canary).

After `canary-ctl deploy order`:
Expected: `lane=canary, value=1`.

---

## Self-Review

**Spec coverage:**
- Spec §"Mirror table" — every Java component has a matching Node task. ✓
- Spec §"Library scope" — all new code under `platform/lib-node/src/observability/`. ✓
- Spec §"Tracing initialization" — Tasks 8 + 11.1 + 12.1. ✓
- Spec §"Metrics emission" — Tasks 3 (CanaryMetrics) + 4 (HTTP middleware) + 5 (Kafka) + 6 (Restate) + 9 (endpoint). ✓
- Spec §"Lane-state gauge" — Task 7. ✓
- Spec §"Metrics endpoint at /actuator/prometheus" — Task 9 + Task 11.2 (mount). ✓
- Spec §"Per-service wiring" — Tasks 11 + 12. ✓

**Placeholders:** None. Every code block is complete.

**Type consistency:**
- `CanaryMetrics` constructor: `(serviceName, registry?)` — consistent across all consumers. ✓
- `currentLane()` return type `"stable" | "canary"` — consistent. ✓
- All metric names match Java: `canary_request_total`, `canary_request_duration_seconds`, `canary_shadow_mismatch_total`, `canary_lane_active`. ✓
- All label sets match Java: `(substrate, service, lane, outcome, target)` for counter; `(substrate, service, lane, target)` for histogram. ✓

**Risks acknowledged in spec:**
- OTel SDK init-ordering: addressed via Task 11.2/12.2's `import "./tracing.js"` as FIRST line.
- Kafka span gap: spec marks as verify-and-fall-back; not a plan task because spec accepts the auto-instrumentation as the default and falls back to manual span creation only if missing — that check is part of E2E (Task 13).
- prom-client global registry collision: CanaryMetrics accepts a `registry?` arg with default `defaultRegister`; each service constructs ONE CanaryMetrics, so no collision.

---

## Out of scope for 5.a-node (handed to 5.b)

- Wiring `measureRestate(...)` at each Restate handler call site in `services/{order,notification}-service/src/restate.ts`. Helper exists; callers don't yet invoke it.
- Wiring `metrics.recordShadowMismatch(...)` at Phase 2/3 shadow-comparison sites.
- Verifying / patching KafkaJS auto-instrumentation if it doesn't emit consumer-side spans.

---

## Plan complete

Plan complete and saved to `docs/superpowers/plans/2026-05-11-canary-release-phase-5-a-node-observability.md`.
