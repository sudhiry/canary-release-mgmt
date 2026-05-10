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
    notificationStore.byId.clear();
  });

  afterEach(() => {
    // Reset kafkaSend so no mock leaks between tests
    configureKafkaSend(null as unknown as KafkaSend);
  });

  it("writes notification to store and calls AuditQueryService.append", async () => {
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

    expect(result.delivered).toBe(true);
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

    // Kafka payload contains the deliveredMessage
    expect(kafkaSendMock).toHaveBeenCalledWith(
      "notifications.events",
      expect.any(String),
      expect.stringContaining(result.deliveredMessage),
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

describe("notification handler variant binding", () => {
  let originalVersion: string | undefined;
  beforeEach(() => {
    originalVersion = process.env.VERSION;
    vi.resetModules();
  });
  afterEach(() => {
    if (originalVersion === undefined) delete process.env.VERSION;
    else process.env.VERSION = originalVersion;
    vi.resetModules();
  });

  it("appends canary suffix when VERSION=canary", async () => {
    process.env.VERSION = "canary";
    vi.resetModules();
    const mod = await import("../restate.js");
    const auditAppend = vi.fn().mockResolvedValue(undefined);
    const ctx = {
      request: () => ({ headers: new Map<string, string>() }),
      serviceClient: vi.fn().mockReturnValue({ append: auditAppend }),
    };
    const result = await mod.notifyHandler(ctx as any, {
      userId: "u1", message: "Order x", orderId: "o1",
    });
    expect(result.deliveredMessage).toBe("Order x [via canary notifier]");
    expect(result.version).toBe("canary");
  });

  it("emits unmodified message when VERSION is not set", async () => {
    delete process.env.VERSION;
    vi.resetModules();
    const mod = await import("../restate.js");
    const auditAppend = vi.fn().mockResolvedValue(undefined);
    const ctx = {
      request: () => ({ headers: new Map<string, string>() }),
      serviceClient: vi.fn().mockReturnValue({ append: auditAppend }),
    };
    const result = await mod.notifyHandler(ctx as any, {
      userId: "u1", message: "Order x", orderId: "o1",
    });
    expect(result.deliveredMessage).toBe("Order x");
    expect(result.version).toBe("stable");
  });

  it("preserves Phase 3.a TerminalError on reject-me", async () => {
    delete process.env.VERSION;
    vi.resetModules();
    const restate = await import("@restatedev/restate-sdk");
    const mod = await import("../restate.js");
    const ctx = {
      request: () => ({ headers: new Map<string, string>() }),
      serviceClient: vi.fn(() => ({ append: vi.fn() })),
    };
    await expect(mod.notifyHandler(ctx as any, {
      userId: "reject-me", message: "x", orderId: "o1",
    })).rejects.toBeInstanceOf(restate.TerminalError);
  });
});
