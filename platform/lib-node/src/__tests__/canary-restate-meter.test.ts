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
