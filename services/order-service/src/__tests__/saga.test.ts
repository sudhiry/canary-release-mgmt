import { describe, it, expect, vi } from "vitest";
import type { AxiosInstance } from "axios";
import { runSaga, type SagaClients } from "../saga.js";

function mockAxios(response: unknown): AxiosInstance {
  return { post: vi.fn().mockResolvedValue({ data: response }) } as unknown as AxiosInstance;
}

describe("runSaga", () => {
  it("calls inventory, payment, and notification in sequence with the right bodies", async () => {
    const reservation = { id: "r_1", sku: "widget", quantity: 1, orderId: "ord_1", status: "reserved" };
    const charge = { id: "ch_1", orderId: "ord_1", amount: 100, status: "succeeded" };
    const notification = { id: "n_1", userId: "u_1", message: "Order ord_1 confirmed", status: "sent" };

    const clients: SagaClients = {
      inventory: mockAxios(reservation),
      payment: mockAxios(charge),
      notification: mockAxios(notification),
    };

    const result = await runSaga("ord_1", { userId: "u_1", sku: "widget", quantity: 1, amount: 100 }, clients);

    expect(result).toEqual({ reservation, charge, notification });
    expect(clients.inventory.post).toHaveBeenCalledWith("/reservations", {
      sku: "widget",
      quantity: 1,
      orderId: "ord_1",
    });
    expect(clients.payment.post).toHaveBeenCalledWith("/charges", { orderId: "ord_1", amount: 100 });
    expect(clients.notification.post).toHaveBeenCalledWith("/notifications", {
      userId: "u_1",
      message: "Order ord_1 confirmed",
      orderId: "ord_1",
    });
  });

  it("propagates the first downstream error (no compensation in 1.3.a)", async () => {
    const failing = { post: vi.fn().mockRejectedValue(new Error("inventory down")) } as unknown as AxiosInstance;
    const clients: SagaClients = {
      inventory: failing,
      payment: mockAxios(null),
      notification: mockAxios(null),
    };

    await expect(
      runSaga("ord_1", { userId: "u_1", sku: "widget", quantity: 1, amount: 100 }, clients),
    ).rejects.toThrow("inventory down");
  });
});
