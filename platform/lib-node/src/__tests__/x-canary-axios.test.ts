import axios from "axios";
import { afterEach, describe, expect, it } from "vitest";
import { setCanary, clearCanary, runWithCanary } from "../x-canary-context.js";
import { attachXCanaryAxiosInterceptor } from "../x-canary-axios.js";

describe("xCanaryAxiosInterceptor", () => {
  afterEach(() => clearCanary());

  it("adds x-canary header when context is canary", async () => {
    const instance = axios.create();
    attachXCanaryAxiosInterceptor(instance);
    await runWithCanary(true, async () => {
      const config = await (instance.interceptors.request as any).handlers[0].fulfilled({
        headers: {},
      });
      expect(config.headers["x-canary"]).toBe("true");
    });
  });

  it("does not add x-canary header when context is not canary", async () => {
    const instance = axios.create();
    attachXCanaryAxiosInterceptor(instance);
    setCanary(false);
    const config = await (instance.interceptors.request as any).handlers[0].fulfilled({
      headers: {},
    });
    expect(config.headers["x-canary"]).toBeUndefined();
  });

  it("does not overwrite an existing x-canary header", async () => {
    const instance = axios.create();
    attachXCanaryAxiosInterceptor(instance);
    await runWithCanary(true, async () => {
      const config = await (instance.interceptors.request as any).handlers[0].fulfilled({
        headers: { "x-canary": "preset" },
      });
      expect(config.headers["x-canary"]).toBe("preset");
    });
  });
});
