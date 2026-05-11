import { describe, it, expect, beforeEach, vi } from "vitest";
import request from "supertest";
import type { AxiosInstance } from "axios";
import { setupHttp } from "../http.js";
import { notificationStore } from "../store.js";
import { createKafkaHealthState, type CanaryMetrics } from "@canary/lib-node";

function mockAxios(): AxiosInstance {
  return {
    post: vi.fn(),
    get: vi.fn(),
  } as unknown as AxiosInstance;
}

describe("HTTP routes", () => {
  beforeEach(() => {
    // Reset store between tests
    (notificationStore as unknown as { byId: Map<string, unknown> }).byId.clear();
  });

  it("POST /notifications delegates to Restate Ingress", async () => {
    const ingressClient = mockAxios();
    const returned = { id: "n_1", userId: "u_1", message: "hi", status: "sent" };
    (ingressClient.post as ReturnType<typeof vi.fn>).mockResolvedValue({ data: returned });

    const app = setupHttp({ ingressClient });

    const body = { userId: "u_1", message: "hi", orderId: "ord_1" };
    const res = await request(app)
      .post("/notifications")
      .set("content-type", "application/json")
      .send(body);

    expect(res.status).toBe(201);
    expect(res.body).toEqual(returned);
    expect(ingressClient.post).toHaveBeenCalledWith("/NotificationService/notify", body);
  });

  it("POST /notifications returns 502 when Ingress call fails", async () => {
    const ingressClient = mockAxios();
    (ingressClient.post as ReturnType<typeof vi.fn>).mockRejectedValue(new Error("network"));

    const app = setupHttp({ ingressClient });

    const res = await request(app)
      .post("/notifications")
      .send({ userId: "u_1", message: "hi", orderId: "ord_1" });

    expect(res.status).toBe(502);
    expect(res.body).toEqual({ error: "ingress_failed" });
  });

  it("GET /health returns 200 with {ok: true}", async () => {
    const app = setupHttp({ ingressClient: mockAxios() });
    const res = await request(app).get("/health");
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ ok: true });
  });

  it("GET /notifications/by-user/:userId reads store directly", async () => {
    notificationStore.put({ id: "n_1", userId: "u_42", message: "hi", status: "sent" });
    notificationStore.put({ id: "n_2", userId: "u_99", message: "ho", status: "sent" });

    const app = setupHttp({ ingressClient: mockAxios() });

    const res = await request(app).get("/notifications/by-user/u_42");

    expect(res.status).toBe(200);
    expect(res.body).toEqual([{ id: "n_1", userId: "u_42", message: "hi", status: "sent" }]);
  });

  it("GET /health on CANARY returns 503 when kafka health state is stale", async () => {
    const staleHealth = createKafkaHealthState(1);
    staleHealth.markAssigned();
    staleHealth.recordHeartbeat();
    await new Promise<void>((r) => setTimeout(r, 10));

    const app = setupHttp({ ingressClient: mockAxios(), kafkaHealth: staleHealth, version: "canary" });
    const res = await request(app).get("/health");
    expect(res.status).toBe(503);
    expect(res.body.ok).toBe(false);
  });

  it("GET /health on STABLE returns 200 even when kafka health state is stale", async () => {
    // Stable must NOT be gated on Kafka health — see canary-overlay.yaml comment.
    // Cold-cluster boot deadlock: lastPollMs==0 → forever 503 if gated.
    const staleHealth = createKafkaHealthState(1);
    staleHealth.markAssigned();
    staleHealth.recordHeartbeat();
    await new Promise<void>((r) => setTimeout(r, 10));

    const app = setupHttp({ ingressClient: mockAxios(), kafkaHealth: staleHealth, version: "stable" });
    const res = await request(app).get("/health");
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ ok: true });
  });

  it("GET /health on STABLE returns 200 even when kafkaHealth has never polled (cold start)", async () => {
    // Specifically reproduces the cold-cluster boot deadlock case:
    // a fresh KafkaHealthState that has never recorded a poll reports
    // ok=false. Stable must still report 200 so it can become Ready and
    // accept traffic that will eventually trigger a poll.
    const coldHealth = createKafkaHealthState(30000);
    expect(coldHealth.report().ok).toBe(false);
    const app = setupHttp({ ingressClient: mockAxios(), kafkaHealth: coldHealth, version: "stable" });
    const res = await request(app).get("/health");
    expect(res.status).toBe(200);
  });

  it("metrics middleware fires for real routes when metrics passed into setupHttp", async () => {
    // Regression test: canaryHttpMetricsMiddleware must be wired BEFORE routes
    // so it intercepts API calls, not just /actuator/prometheus.
    // Use a stub to avoid prom-client duplicate-registration issues across test runs.
    const recordHttp = vi.fn();
    const metrics = { recordHttp } as unknown as CanaryMetrics;

    const ingressClient = mockAxios();
    const returned = { id: "n_1", userId: "u_1", message: "hi", status: "sent" };
    (ingressClient.post as ReturnType<typeof vi.fn>).mockResolvedValue({ data: returned });

    const app = setupHttp({ ingressClient, metrics });

    await request(app)
      .post("/notifications")
      .send({ userId: "u_1", message: "hi", orderId: "ord_1" });

    // recordHttp must have been called — proves middleware ran for a real route
    expect(recordHttp).toHaveBeenCalledOnce();
    const [target, outcome] = (recordHttp as ReturnType<typeof vi.fn>).mock.calls[0];
    expect(target).toContain("/notifications");
    expect(outcome).toBe("success");
  });
});
