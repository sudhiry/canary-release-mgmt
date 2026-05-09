import { describe, it, expect, beforeEach, vi } from "vitest";
import request from "supertest";
import type { AxiosInstance } from "axios";
import { setupHttp } from "../http.js";
import { notificationStore } from "../store.js";
import { createKafkaHealthState } from "@canary/lib-node";

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

  it("GET /health returns 503 when kafka health state is stale", async () => {
    // Create a health state with 1ms timeout, record a poll, then let it go stale.
    const staleHealth = createKafkaHealthState(1);
    staleHealth.recordPoll();
    await new Promise<void>((r) => setTimeout(r, 10));

    const app = setupHttp({ ingressClient: mockAxios(), kafkaHealth: staleHealth });
    const res = await request(app).get("/health");
    expect(res.status).toBe(503);
    expect(res.body.ok).toBe(false);
  });
});
