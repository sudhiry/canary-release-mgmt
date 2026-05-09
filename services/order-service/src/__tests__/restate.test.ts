import { describe, it, expect, vi } from "vitest";
import { setupRestate, checkoutSaga, checkoutSagaRunHandler } from "../restate.js";

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

    await setupRestate({ registerHandlers: false, port: 9084 });

    expect(restate.endpoint).not.toHaveBeenCalled();
  });

  it("calls endpoint().bind(checkoutSaga).listen(port) when registerHandlers=true", async () => {
    const restate = await import("@restatedev/restate-sdk");
    const bindMock = vi.fn().mockReturnThis();
    const listenMock = vi.fn().mockResolvedValue(undefined);
    (restate.endpoint as ReturnType<typeof vi.fn>).mockReturnValue({
      bind: bindMock,
      listen: listenMock,
    });

    await setupRestate({ registerHandlers: true, port: 9084 });

    expect(restate.endpoint).toHaveBeenCalledOnce();
    expect(bindMock).toHaveBeenCalledWith(checkoutSaga);
    expect(listenMock).toHaveBeenCalledWith(9084);
  });
});

describe("CheckoutSaga.run handler stub", () => {
  it("returns a stub-completed Order", async () => {
    // ctx.request().headers is a ReadonlyMap<string, string> in SDK 1.14.
    const ctx = {
      request: () => ({ headers: new Map<string, string>() }),
    };

    const result = (await checkoutSagaRunHandler(
      ctx as unknown as import("@restatedev/restate-sdk").WorkflowContext,
      { userId: "u_1", sku: "widget", quantity: 1, amount: 100 },
    )) as { status: string; userId: string };

    expect(result.status).toBe("stub-completed");
    expect(result.userId).toBe("u_1");
  });
});
