import { describe, it, expect, afterEach } from "vitest";
import { isCanary, runWithCanary, setCanary, clearCanary } from "../x-canary-context.js";

describe("XCanaryContext", () => {
  afterEach(() => clearCanary());

  it("is initially false", () => {
    expect(isCanary()).toBe(false);
  });

  it("setCanary(true) flips the flag", () => {
    setCanary(true);
    expect(isCanary()).toBe(true);
  });

  it("clearCanary resets to false", () => {
    setCanary(true);
    clearCanary();
    expect(isCanary()).toBe(false);
  });

  it("runWithCanary exposes the flag inside the callback only", async () => {
    expect(isCanary()).toBe(false);
    await runWithCanary(true, async () => {
      expect(isCanary()).toBe(true);
    });
    expect(isCanary()).toBe(false);
  });

  it("runWithCanary survives rejection", async () => {
    await expect(
      runWithCanary(true, async () => {
        throw new Error("boom");
      })
    ).rejects.toThrow("boom");
    expect(isCanary()).toBe(false);
  });
});
