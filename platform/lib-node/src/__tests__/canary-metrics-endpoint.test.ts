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
