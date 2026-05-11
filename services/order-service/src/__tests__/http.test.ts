import { describe, it, expect, beforeEach, vi } from "vitest";
import request from "supertest";
import type { AxiosInstance } from "axios";
import { setupHttp } from "../http.js";
import { orderStore } from "../store.js";
import type { CanaryMetrics } from "@canary/lib-node";

/**
 * Build a minimal AxiosInstance-shaped stub. http.ts only uses `post<T>(url, body)`,
 * so we expose just that method (vi.fn) and let the test set its return per case.
 * Avoids the axios-mock-adapter dev dep (not currently in package.json).
 */
function makeIngressClient(postImpl?: (url: string, body: unknown) => Promise<unknown>): AxiosInstance {
  return {
    post: vi.fn(postImpl ?? (async () => ({ data: null }))),
  } as unknown as AxiosInstance;
}

describe("HTTP routes", () => {
  beforeEach(() => {
    (orderStore as unknown as { byId: Map<string, unknown> }).byId.clear();
  });

  it("POST /api/orders posts to Restate Ingress and returns the completed order", async () => {
    const ingressClient = makeIngressClient(async (url) => {
      // url shape: /CheckoutSagaStable/<orderId>/run (no x-canary header → Stable)
      const orderId = url.split("/")[2];
      return {
        data: {
          id: orderId,
          userId: "u_1",
          sku: "widget",
          quantity: 1,
          amount: 100,
          status: "completed",
          auditTrail: ["saga@stable"],
        },
      };
    });
    const app = setupHttp({ ingressClient });

    const res = await request(app)
      .post("/api/orders")
      .send({ userId: "u_1", sku: "widget", quantity: 1, amount: 100 });

    expect(res.status).toBe(201);
    expect(res.body.status).toBe("completed");
    expect(res.body.userId).toBe("u_1");
    expect(orderStore.findById(res.body.id)?.status).toBe("completed");

    expect(ingressClient.post).toHaveBeenCalledOnce();
    const [url, body] = (ingressClient.post as ReturnType<typeof vi.fn>).mock.calls[0];
    expect(url).toMatch(/^\/CheckoutSagaStable\/[0-9a-f-]+\/run$/);
    expect(body).toEqual({ userId: "u_1", sku: "widget", quantity: 1, amount: 100 });
  });

  it("POST /api/orders returns 502 + status=failed when ingress returns failed status", async () => {
    const ingressClient = makeIngressClient(async (url) => {
      const orderId = url.split("/")[2];
      return {
        data: {
          id: orderId,
          userId: "u_1",
          sku: "widget",
          quantity: 1,
          amount: 100,
          status: "failed",
          auditTrail: [],
        },
      };
    });
    const app = setupHttp({ ingressClient });

    const res = await request(app)
      .post("/api/orders")
      .send({ userId: "u_1", sku: "widget", quantity: 1, amount: 100 });

    expect(res.status).toBe(502);
    expect(res.body.error).toBe("saga_failed");
    expect(res.body.order.status).toBe("failed");
  });

  it("POST /api/orders returns 502 + ingress_failed when ingress call rejects", async () => {
    const ingressClient = makeIngressClient(async () => {
      throw new Error("ingress unreachable");
    });
    const app = setupHttp({ ingressClient });

    const res = await request(app)
      .post("/api/orders")
      .send({ userId: "u_1", sku: "widget", quantity: 1, amount: 100 });

    expect(res.status).toBe(502);
    expect(res.body.error).toBe("ingress_failed");
    expect(res.body.order.status).toBe("failed");
  });

  it("emits orders.events via kafkaSend after a successful saga", async () => {
    const ingressClient = makeIngressClient(async (url) => {
      const orderId = url.split("/")[2];
      return {
        data: {
          id: orderId,
          userId: "u_1",
          sku: "widget",
          quantity: 1,
          amount: 100,
          status: "completed",
          auditTrail: ["saga@stable"],
        },
      };
    });
    const kafkaSend = vi.fn().mockResolvedValue(undefined);
    const app = setupHttp({ ingressClient, kafkaSend });

    const res = await request(app)
      .post("/api/orders")
      .send({ userId: "u_1", sku: "widget", quantity: 1, amount: 100 });

    expect(res.status).toBe(201);
    expect(kafkaSend).toHaveBeenCalledOnce();
    const [topic, key, value] = kafkaSend.mock.calls[0];
    expect(topic).toBe("orders.events");
    expect(key).toBe(res.body.id);
    expect(value).toContain(res.body.id);
  });

  it("GET /api/orders/:id returns 200 when found", async () => {
    orderStore.put({ id: "ord_1", userId: "u_1", sku: "widget", quantity: 1, amount: 100, status: "completed", auditTrail: [] });

    const app = setupHttp({ ingressClient: makeIngressClient() });

    const res = await request(app).get("/api/orders/ord_1");

    expect(res.status).toBe(200);
    expect(res.body.id).toBe("ord_1");
  });

  it("GET /api/orders/:id returns 404 when missing", async () => {
    const app = setupHttp({ ingressClient: makeIngressClient() });

    const res = await request(app).get("/api/orders/nope");

    expect(res.status).toBe(404);
  });

  it("posts to /CheckoutSagaCanary when x-canary: true", async () => {
    const ingressClient = makeIngressClient(async (url) => {
      const orderId = url.split("/")[2];
      return {
        data: {
          id: orderId,
          userId: "u_1",
          sku: "widget",
          quantity: 1,
          amount: 100,
          status: "completed",
          auditTrail: ["saga@canary"],
        },
      };
    });
    const app = setupHttp({ ingressClient });

    const res = await request(app)
      .post("/api/orders")
      .set("x-canary", "true")
      .send({ userId: "u_1", sku: "widget", quantity: 1, amount: 100 });

    expect(res.status).toBe(201);
    expect(res.body.auditTrail).toContain("saga@canary");
    expect(ingressClient.post).toHaveBeenCalledOnce();
    const [url] = (ingressClient.post as ReturnType<typeof vi.fn>).mock.calls[0];
    expect(url).toMatch(/^\/CheckoutSagaCanary\/[0-9a-f-]+\/run$/);
  });

  it("posts to /CheckoutSagaStable when x-canary absent", async () => {
    const ingressClient = makeIngressClient(async (url) => {
      const orderId = url.split("/")[2];
      return {
        data: {
          id: orderId,
          userId: "u_1",
          sku: "widget",
          quantity: 1,
          amount: 100,
          status: "completed",
          auditTrail: ["saga@stable"],
        },
      };
    });
    const app = setupHttp({ ingressClient });

    const res = await request(app)
      .post("/api/orders")
      .send({ userId: "u_1", sku: "widget", quantity: 1, amount: 100 });

    expect(res.status).toBe(201);
    expect(res.body.auditTrail).toContain("saga@stable");
    expect(ingressClient.post).toHaveBeenCalledOnce();
    const [url] = (ingressClient.post as ReturnType<typeof vi.fn>).mock.calls[0];
    expect(url).toMatch(/^\/CheckoutSagaStable\/[0-9a-f-]+\/run$/);
  });

  it("posts to /CheckoutSagaStable when x-canary is 'false'", async () => {
    const ingressClient = makeIngressClient(async (url) => {
      const orderId = url.split("/")[2];
      return {
        data: {
          id: orderId,
          userId: "u_1",
          sku: "widget",
          quantity: 1,
          amount: 100,
          status: "completed",
          auditTrail: ["saga@stable"],
        },
      };
    });
    const app = setupHttp({ ingressClient });

    const res = await request(app)
      .post("/api/orders")
      .set("x-canary", "false")
      .send({ userId: "u_1", sku: "widget", quantity: 1, amount: 100 });

    expect(res.status).toBe(201);
    expect(res.body.auditTrail).toContain("saga@stable");
    expect(ingressClient.post).toHaveBeenCalledOnce();
    const [url] = (ingressClient.post as ReturnType<typeof vi.fn>).mock.calls[0];
    expect(url).toMatch(/^\/CheckoutSagaStable\/[0-9a-f-]+\/run$/);
  });

  it("GET /health returns 200 with {ok: true}", async () => {
    const app = setupHttp({ ingressClient: makeIngressClient() });
    const res = await request(app).get("/health");
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ ok: true });
  });

  it("GET /health on CANARY returns 503 when kafka health state is stale", async () => {
    const { createKafkaHealthState } = await import("@canary/lib-node");
    const staleHealth = createKafkaHealthState(1);
    staleHealth.markAssigned();
    staleHealth.recordHeartbeat();
    await new Promise<void>((r) => setTimeout(r, 10));

    const app = setupHttp({
      ingressClient: makeIngressClient(),
      kafkaHealth: staleHealth,
      version: "canary",
    });
    const res = await request(app).get("/health");
    expect(res.status).toBe(503);
    expect(res.body.ok).toBe(false);
  });

  it("GET /health on STABLE returns 200 even when kafkaHealth has never polled (cold start)", async () => {
    // Reproduces the cold-cluster boot deadlock case: stable must NOT be
    // gated on Kafka health, so a fresh pod with lastPollMs==0 still
    // becomes Ready. See deploy/helm/values/canary-overlay.yaml comment.
    const { createKafkaHealthState } = await import("@canary/lib-node");
    const coldHealth = createKafkaHealthState(30000);
    expect(coldHealth.report().ok).toBe(false);

    const app = setupHttp({
      ingressClient: makeIngressClient(),
      kafkaHealth: coldHealth,
      version: "stable",
    });
    const res = await request(app).get("/health");
    expect(res.status).toBe(200);
  });

  it("metrics middleware fires for real routes when metrics passed into setupHttp", async () => {
    // Regression test: canaryHttpMetricsMiddleware must be wired BEFORE routes
    // so it intercepts API calls, not just /actuator/prometheus.
    // Use a stub to avoid prom-client duplicate-registration issues across test runs.
    const recordHttp = vi.fn();
    const metrics = { recordHttp } as unknown as CanaryMetrics;

    const ingressClient = makeIngressClient(async (url) => {
      const orderId = url.split("/")[2];
      return {
        data: {
          id: orderId, userId: "u_1", sku: "widget",
          quantity: 1, amount: 100, status: "completed", auditTrail: [],
        },
      };
    });

    const app = setupHttp({ ingressClient, metrics });

    await request(app)
      .post("/api/orders")
      .send({ userId: "u_1", sku: "widget", quantity: 1, amount: 100 });

    // recordHttp must have been called — proves middleware ran for a real route
    expect(recordHttp).toHaveBeenCalledOnce();
    const [target, outcome] = (recordHttp as ReturnType<typeof vi.fn>).mock.calls[0];
    expect(target).toContain("/api/orders");
    expect(outcome).toBe("success");
  });
});
