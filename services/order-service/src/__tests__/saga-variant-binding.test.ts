import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";

describe("saga variant binding", () => {
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

  it("binds CheckoutSagaStable when VERSION is unset or 'stable'", async () => {
    delete process.env.VERSION;
    const mod = await import("../restate.js");
    expect(mod.checkoutSaga.name).toBe("CheckoutSagaStable");
  });

  it("binds CheckoutSagaCanary when VERSION is 'canary'", async () => {
    process.env.VERSION = "canary";
    const mod = await import("../restate.js");
    expect(mod.checkoutSaga.name).toBe("CheckoutSagaCanary");
  });
});
