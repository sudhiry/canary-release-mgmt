import { describe, it, expect, beforeEach, vi } from "vitest";
import request from "supertest";
import type { AxiosInstance } from "axios";
import { setupHttp } from "../http.js";
import { orderStore } from "../store.js";

function mockAxios(response: unknown): AxiosInstance {
  return { post: vi.fn().mockResolvedValue({ data: response }) } as unknown as AxiosInstance;
}

describe("HTTP routes", () => {
  beforeEach(() => {
    (orderStore as unknown as { byId: Map<string, unknown> }).byId.clear();
  });

  it("POST /api/orders runs the saga and returns the completed order", async () => {
    const app = setupHttp({
      clients: {
        inventory: mockAxios({ id: "r_1", sku: "widget", quantity: 1, orderId: "?", status: "reserved" }),
        payment: mockAxios({ id: "ch_1", orderId: "?", amount: 100, status: "succeeded" }),
        notification: mockAxios({ id: "n_1", userId: "u_1", message: "x", status: "sent" }),
      },
    });

    const res = await request(app)
      .post("/api/orders")
      .send({ userId: "u_1", sku: "widget", quantity: 1, amount: 100 });

    expect(res.status).toBe(201);
    expect(res.body.status).toBe("completed");
    expect(res.body.userId).toBe("u_1");
    expect(orderStore.findById(res.body.id)?.status).toBe("completed");
  });

  it("POST /api/orders returns 502 + status=failed on downstream failure", async () => {
    const failing = { post: vi.fn().mockRejectedValue(new Error("inventory down")) } as unknown as AxiosInstance;
    const app = setupHttp({
      clients: {
        inventory: failing,
        payment: mockAxios(null),
        notification: mockAxios(null),
      },
    });

    const res = await request(app)
      .post("/api/orders")
      .send({ userId: "u_1", sku: "widget", quantity: 1, amount: 100 });

    expect(res.status).toBe(502);
    expect(res.body.error).toBe("saga_failed");
    expect(res.body.order.status).toBe("failed");
  });

  it("emits orders.events via kafkaSend when configured", async () => {
    const kafkaSend = vi.fn().mockResolvedValue(undefined);
    const app = setupHttp({
      clients: {
        inventory: mockAxios({ id: "r_1", sku: "widget", quantity: 1, orderId: "?", status: "reserved" }),
        payment: mockAxios({ id: "ch_1", orderId: "?", amount: 100, status: "succeeded" }),
        notification: mockAxios({ id: "n_1", userId: "u_1", message: "x", status: "sent" }),
      },
      kafkaSend,
    });

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
    orderStore.put({ id: "ord_1", userId: "u_1", sku: "widget", quantity: 1, amount: 100, status: "completed" });

    const app = setupHttp({
      clients: { inventory: mockAxios(null), payment: mockAxios(null), notification: mockAxios(null) },
    });

    const res = await request(app).get("/api/orders/ord_1");

    expect(res.status).toBe(200);
    expect(res.body.id).toBe("ord_1");
  });

  it("GET /api/orders/:id returns 404 when missing", async () => {
    const app = setupHttp({
      clients: { inventory: mockAxios(null), payment: mockAxios(null), notification: mockAxios(null) },
    });

    const res = await request(app).get("/api/orders/nope");

    expect(res.status).toBe(404);
  });

  it("GET /health returns 200 with {ok: true}", async () => {
    const app = setupHttp({
      clients: {
        inventory: { post: () => Promise.reject() } as unknown as AxiosInstance,
        payment: { post: () => Promise.reject() } as unknown as AxiosInstance,
        notification: { post: () => Promise.reject() } as unknown as AxiosInstance,
      },
    });
    const res = await request(app).get("/health");
    expect(res.status).toBe(200);
    expect(res.body).toEqual({ ok: true });
  });
});
