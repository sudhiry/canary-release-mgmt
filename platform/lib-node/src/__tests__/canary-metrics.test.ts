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
