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
