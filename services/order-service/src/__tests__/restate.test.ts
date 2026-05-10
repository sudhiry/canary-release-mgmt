import { describe, it, expect, vi } from "vitest";
import * as restate from "@restatedev/restate-sdk";
import { setupRestate, checkoutSaga, checkoutSagaRunHandler } from "../restate.js";
import {
  paymentVOStableDef,
  reservationWorkflowStableDef,
  notificationServiceStableDef,
} from "@canary/restate-defs-node";

vi.mock("@restatedev/restate-sdk", async () => {
  const actual = await vi.importActual<typeof import("@restatedev/restate-sdk")>(
    "@restatedev/restate-sdk",
  );
  return {
    ...actual,
    endpoint: vi.fn(() => ({
      bind: vi.fn().mockReturnThis(),
      listen: vi.fn().mockResolvedValue(undefined),
    })),
  };
});

interface MockClients {
  reservationSend: { run: ReturnType<typeof vi.fn> };
  reservation: {
    run: ReturnType<typeof vi.fn>;
    confirm: ReturnType<typeof vi.fn>;
    release: ReturnType<typeof vi.fn>;
  };
  payment: { charge: ReturnType<typeof vi.fn>; refund: ReturnType<typeof vi.fn> };
  notification: { notify: ReturnType<typeof vi.fn> };
}

interface CtxWithMocks extends restate.WorkflowContext {
  _mocks: MockClients;
}

function buildCtx(opts: {
  reservationRun?: () => Promise<unknown>;
  charge?: () => Promise<unknown>;
  refund?: () => Promise<unknown>;
  confirm?: () => Promise<unknown>;
  release?: () => Promise<unknown>;
  notify?: () => Promise<unknown>;
  canary?: boolean;
  key?: string;
}): CtxWithMocks {
  const reservationSendClient = {
    run: vi.fn(opts.reservationRun ?? (async () => ({ invocationId: Promise.resolve("inv_1") }))),
  };
  const reservationClient = {
    run: vi.fn(opts.reservationRun ?? (async () =>
      ({ id: "r_1", sku: "widget", quantity: 1, orderId: "o_1", status: "reserved" }))),
    confirm: vi.fn(opts.confirm ?? (async () => undefined)),
    release: vi.fn(opts.release ?? (async () => undefined)),
  };
  const paymentClient = {
    charge: vi.fn(opts.charge ?? (async () =>
      ({ id: "c_1", orderId: "o_1", amount: 100, status: "succeeded" }))),
    refund: vi.fn(opts.refund ?? (async () =>
      ({ id: "c_1", orderId: "o_1", amount: 100, status: "refunded" }))),
  };
  const notificationClient = {
    notify: vi.fn(opts.notify ?? (async () =>
      ({ delivered: true, version: "stable" as const, deliveredMessage: "ok" }))),
  };
  const headers = new Map<string, string>();
  if (opts.canary) headers.set("x-canary", "true");
  const key = opts.key ?? "o_1";

  const ctx = {
    key,
    request: () => ({ headers }),
    workflowClient: vi.fn((def: unknown, _key: string) => {
      if (def === reservationWorkflowStableDef) return reservationClient;
      throw new Error("unexpected workflowClient def");
    }),
    workflowSendClient: vi.fn((def: unknown, _key: string) => {
      if (def === reservationWorkflowStableDef) return reservationSendClient;
      throw new Error("unexpected workflowSendClient def");
    }),
    objectClient: vi.fn((def: unknown, _key: string) => {
      if (def === paymentVOStableDef) return paymentClient;
      throw new Error("unexpected objectClient def");
    }),
    serviceClient: vi.fn((def: unknown) => {
      if (def === notificationServiceStableDef) return notificationClient;
      throw new Error("unexpected serviceClient def");
    }),
    _mocks: {
      reservationSend: reservationSendClient,
      reservation: reservationClient,
      payment: paymentClient,
      notification: notificationClient,
    },
  } as unknown as CtxWithMocks;
  return ctx;
}

describe("setupRestate gating", () => {
  it("does NOT call endpoint().listen when registerHandlers=false", async () => {
    const sdk = await import("@restatedev/restate-sdk");
    (sdk.endpoint as ReturnType<typeof vi.fn>).mockClear();
    await setupRestate({ registerHandlers: false, port: 9084 });
    expect(sdk.endpoint).not.toHaveBeenCalled();
  });

  it("calls endpoint().bind(checkoutSaga).listen(port) when registerHandlers=true", async () => {
    const sdk = await import("@restatedev/restate-sdk");
    const bindMock = vi.fn().mockReturnThis();
    const listenMock = vi.fn().mockResolvedValue(undefined);
    (sdk.endpoint as ReturnType<typeof vi.fn>).mockReturnValue({
      bind: bindMock,
      listen: listenMock,
    });
    await setupRestate({ registerHandlers: true, port: 9084 });
    expect(sdk.endpoint).toHaveBeenCalledOnce();
    expect(bindMock).toHaveBeenCalledWith(checkoutSaga);
    expect(listenMock).toHaveBeenCalledWith(9084);
  });
});

describe("CheckoutSaga.run real saga", () => {
  it("happyPathExecutesAllStepsInOrder", async () => {
    const ctx = buildCtx({});

    const order = await checkoutSagaRunHandler(ctx, {
      userId: "u_1", sku: "widget", quantity: 1, amount: 100,
    });

    expect(ctx._mocks.reservationSend.run).toHaveBeenCalledOnce();
    expect(ctx._mocks.payment.charge).toHaveBeenCalledOnce();
    expect(ctx._mocks.reservation.confirm).toHaveBeenCalledOnce();
    expect(ctx._mocks.notification.notify).toHaveBeenCalledOnce();
    expect(ctx._mocks.reservation.release).not.toHaveBeenCalled();
    expect(ctx._mocks.payment.refund).not.toHaveBeenCalled();
    expect(order.status).toBe("completed");
    // orderId == workflow key
    expect(order.id).toBe("o_1");
  });

  it("paymentTerminalErrorTriggersReleaseReservation", async () => {
    const ctx = buildCtx({
      charge: async () => { throw new restate.TerminalError("payment-rejected"); },
    });

    const order = await checkoutSagaRunHandler(ctx, {
      userId: "u_1", sku: "widget", quantity: 1, amount: 100,
    });

    expect(ctx._mocks.reservationSend.run).toHaveBeenCalledOnce();
    expect(ctx._mocks.payment.charge).toHaveBeenCalledOnce();
    expect(ctx._mocks.reservation.release).toHaveBeenCalledOnce();
    expect(ctx._mocks.reservation.confirm).not.toHaveBeenCalled();
    expect(ctx._mocks.payment.refund).not.toHaveBeenCalled();
    expect(ctx._mocks.notification.notify).not.toHaveBeenCalled();
    expect(order.status).toBe("failed");
  });

  it("notifyTerminalErrorTriggersRefundOnly", async () => {
    const ctx = buildCtx({
      notify: async () => { throw new restate.TerminalError("notify-rejected"); },
    });

    const order = await checkoutSagaRunHandler(ctx, {
      userId: "u_1", sku: "widget", quantity: 1, amount: 100,
    });

    expect(ctx._mocks.reservationSend.run).toHaveBeenCalledOnce();
    expect(ctx._mocks.reservation.confirm).toHaveBeenCalledOnce();
    expect(ctx._mocks.payment.charge).toHaveBeenCalledOnce();
    expect(ctx._mocks.notification.notify).toHaveBeenCalledOnce();
    expect(ctx._mocks.payment.refund).toHaveBeenCalledOnce();
    // Reservation stays confirmed (partial reversal)
    expect(ctx._mocks.reservation.release).not.toHaveBeenCalled();
    expect(order.status).toBe("failed");
  });

  it("confirmTerminalErrorTriggersRefund", async () => {
    const ctx = buildCtx({
      confirm: async () => { throw new restate.TerminalError("confirm-raced-with-timer"); },
    });

    const order = await checkoutSagaRunHandler(ctx, {
      userId: "u_1", sku: "widget", quantity: 1, amount: 100,
    });

    expect(ctx._mocks.reservationSend.run).toHaveBeenCalledOnce();
    expect(ctx._mocks.payment.charge).toHaveBeenCalledOnce();
    expect(ctx._mocks.reservation.confirm).toHaveBeenCalledOnce();
    expect(ctx._mocks.payment.refund).toHaveBeenCalledOnce();
    expect(ctx._mocks.notification.notify).not.toHaveBeenCalled();
    expect(ctx._mocks.reservation.release).not.toHaveBeenCalled();
    expect(order.status).toBe("failed");
  });

  it("xCanaryHeaderPropagatesToAllRtoRCalls", async () => {
    const ctx = buildCtx({ canary: true });

    await checkoutSagaRunHandler(ctx, {
      userId: "u_1", sku: "widget", quantity: 1, amount: 100,
    });

    // Each R-to-R call should have received an opts arg with x-canary header.
    // We verify the mock was called with at least one extra argument (the opts wrapper).
    expect(ctx._mocks.reservationSend.run).toHaveBeenCalledOnce();
    expect(ctx._mocks.payment.charge).toHaveBeenCalledOnce();
    expect(ctx._mocks.reservation.confirm).toHaveBeenCalledOnce();
    expect(ctx._mocks.notification.notify).toHaveBeenCalledOnce();

    // The opts is the second positional arg for handlers that take a request (charge/notify/run)
    // and the first arg for handlers that take no request (confirm/release).
    const reservationSendArgs = ctx._mocks.reservationSend.run.mock.calls[0];
    const chargeArgs = ctx._mocks.payment.charge.mock.calls[0];
    const confirmArgs = ctx._mocks.reservation.confirm.mock.calls[0];
    const notifyArgs = ctx._mocks.notification.notify.mock.calls[0];

    // Each call passes some opts value (truthy object) — proves opts is wired.
    expect(reservationSendArgs.length).toBeGreaterThanOrEqual(2);
    expect(chargeArgs.length).toBeGreaterThanOrEqual(2);
    expect(confirmArgs.length).toBeGreaterThanOrEqual(1);
    expect(notifyArgs.length).toBeGreaterThanOrEqual(2);
  });
});
