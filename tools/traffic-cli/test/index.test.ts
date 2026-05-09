import { describe, expect, it, vi } from "vitest";

vi.mock("axios", () => ({
  default: { post: vi.fn() },
}));

import axios from "axios";
import { sendOrder } from "../src/index.js";

const postMock = vi.mocked(axios.post);

describe("traffic-cli sendOrder", () => {
  it("default: no x-canary header", async () => {
    postMock.mockResolvedValue({ status: 201, data: { id: "ord-1" }, headers: {} });
    await sendOrder({ url: "http://localhost:8080", canary: false, user: "u1", sku: "sku-1", quantity: 1, amount: 100 });
    const [, , cfg] = postMock.mock.calls[0];
    expect((cfg?.headers as Record<string, string>)["x-canary"]).toBeUndefined();
  });

  it("--canary attaches the header", async () => {
    postMock.mockResolvedValue({ status: 201, data: { id: "ord-2" }, headers: {} });
    await sendOrder({ url: "http://localhost:8080", canary: true, user: "u1", sku: "sku-1", quantity: 1, amount: 100 });
    const [, , cfg] = postMock.mock.calls[0];
    expect((cfg?.headers as Record<string, string>)["x-canary"]).toBe("true");
  });

  it("returns response status, data, headers", async () => {
    postMock.mockResolvedValue({ status: 201, data: { id: "ord-3" }, headers: { server: "envoy" } });
    const r = await sendOrder({ url: "http://localhost:8080", canary: false, user: "u1", sku: "sku-1", quantity: 1, amount: 100 });
    expect(r.status).toBe(201);
    expect(r.data).toEqual({ id: "ord-3" });
    expect(r.headers).toEqual({ server: "envoy" });
  });
});
