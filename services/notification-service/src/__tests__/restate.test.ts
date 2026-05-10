import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { setupRestate, notificationService, notifyHandler, configureKafkaSend, type KafkaSend } from "../restate.js";
import { notificationStore } from "../store.js";

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

describe("setupRestate gating", () => {
  it("does NOT call endpoint().listen when registerHandlers=false", async () => {
    const restate = await import("@restatedev/restate-sdk");
    (restate.endpoint as ReturnType<typeof vi.fn>).mockClear();

    await setupRestate({ registerHandlers: false, port: 9085 });

    expect(restate.endpoint).not.toHaveBeenCalled();
  });

  it("calls endpoint().bind(svc).listen(port) when registerHandlers=true", async () => {
    const restate = await import("@restatedev/restate-sdk");
    const bindMock = vi.fn().mockReturnThis();
    const listenMock = vi.fn().mockResolvedValue(undefined);
    (restate.endpoint as ReturnType<typeof vi.fn>).mockReturnValue({
      bind: bindMock,
      listen: listenMock,
    });

    await setupRestate({ registerHandlers: true, port: 9085 });

    expect(restate.endpoint).toHaveBeenCalledOnce();
    expect(bindMock).toHaveBeenCalledWith(notificationService);
    expect(listenMock).toHaveBeenCalledWith(9085);
  });
});

describe("NotificationService.notify handler", () => {
  beforeEach(() => {
    (notificationStore as unknown as { byId: Map<string, unknown> }).byId.clear();
  });

  afterEach(() => {
    // Reset kafkaSend so no mock leaks between tests
    configureKafkaSend(null as unknown as KafkaSend);
  });

  it("writes Notification to store and calls AuditQueryService.append", async () => {
    const auditAppend = vi.fn().mockResolvedValue(undefined);
    // ctx.request().headers is a ReadonlyMap<string, string> in SDK 1.14.
    const ctx = {
      request: () => ({ headers: new Map<string, string>() }),
      serviceClient: vi.fn().mockReturnValue({ append: auditAppend }),
    };

    const result = (await notifyHandler(
      ctx as unknown as import("@restatedev/restate-sdk").Context,
      { userId: "u_1", message: "hi", orderId: "ord_1" },
    ));

    expect(result.status).toBe("sent");
    expect(notificationStore.byUserId("u_1")).toHaveLength(1);

    expect(auditAppend).toHaveBeenCalledOnce();
    const [event, opts] = auditAppend.mock.calls[0] as [unknown, { getOpts(): { headers?: Record<string, string> } }];
    expect(event).toMatchObject({ aggregate: "notification", action: "sent", correlationId: "ord_1" });
    // Without x-canary in request headers, opts should NOT stamp the header.
    expect(opts.getOpts().headers?.["x-canary"]).toBeUndefined();
  });

  it("stamps x-canary on audit call when incoming request had it", async () => {
    const auditAppend = vi.fn().mockResolvedValue(undefined);
    const ctx = {
      request: () => ({ headers: new Map([["x-canary", "true"]]) }),
      serviceClient: vi.fn().mockReturnValue({ append: auditAppend }),
    };

    await notifyHandler(
      ctx as unknown as import("@restatedev/restate-sdk").Context,
      { userId: "u_1", message: "hi", orderId: "ord_1" },
    );

    const [, opts] = auditAppend.mock.calls[0] as [unknown, { getOpts(): { headers?: Record<string, string> } }];
    expect(opts.getOpts().headers?.["x-canary"]).toBe("true");
  });

  it("emits notifications.events via the configured kafkaSend", async () => {
    const kafkaSendMock = vi.fn().mockResolvedValue(undefined);
    configureKafkaSend(kafkaSendMock);

    const auditAppend = vi.fn().mockResolvedValue(undefined);
    const ctx = {
      request: () => ({ headers: new Map<string, string>() }),
      serviceClient: vi.fn().mockReturnValue({ append: auditAppend }),
    };

    const result = await notifyHandler(
      ctx as unknown as import("@restatedev/restate-sdk").Context,
      { userId: "u_1", message: "hi", orderId: "ord_1" },
    );

    expect(kafkaSendMock).toHaveBeenCalledWith(
      "notifications.events",
      result.id,
      expect.stringContaining(result.id),
    );
  });

  it("notifyHandler throws TerminalError when userId is 'reject-me'", async () => {
    const ctx = {
      request: () => ({ headers: new Map<string, string>() }),
      serviceClient: vi.fn(() => ({ append: vi.fn() })),
    };
    await expect(
      notifyHandler(
        ctx as unknown as import("@restatedev/restate-sdk").Context,
        { userId: "reject-me", message: "x", orderId: "o_1" },
      ),
    ).rejects.toThrow(/rejected for test driver/);
  });
});
