import { afterEach, describe, expect, it } from "vitest";
import { runWithCanary, clearCanary } from "../x-canary-context.js";
import { applyXCanaryToRestateOptions } from "../x-canary-restate.js";
import type { ClientCallOptions } from "@restatedev/restate-sdk";

describe("applyXCanaryToRestateOptions", () => {
  afterEach(() => clearCanary());

  it("attaches x-canary header to the options object when context is canary", async () => {
    await runWithCanary(true, async () => {
      const options: ClientCallOptions<unknown, unknown> = { headers: {} };
      const out = applyXCanaryToRestateOptions(options);
      expect(out.headers!["x-canary"]).toBe("true");
    });
  });

  it("returns options unchanged when context is not canary", () => {
    const options: ClientCallOptions<unknown, unknown> = { headers: {} };
    const out = applyXCanaryToRestateOptions(options);
    expect(out.headers!["x-canary"]).toBeUndefined();
  });

  it("does not overwrite an existing x-canary header", async () => {
    await runWithCanary(true, async () => {
      const options: ClientCallOptions<unknown, unknown> = {
        headers: { "x-canary": "preset" },
      };
      const out = applyXCanaryToRestateOptions(options);
      expect(out.headers!["x-canary"]).toBe("preset");
    });
  });
});
