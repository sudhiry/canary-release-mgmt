import { describe, expect, it, vi } from "vitest";
import { attachXServedChainAxiosInterceptor } from "../x-served-chain-axios.js";
import { collectTokens, runWithChain } from "../x-served-chain-context.js";

function mockAxiosInstance(): { interceptors: { response: { use: ReturnType<typeof vi.fn> } } } {
  return {
    interceptors: {
      response: { use: vi.fn() },
    },
  };
}

describe("attachXServedChainAxiosInterceptor", () => {
  it("registers a response interceptor", () => {
    const ax = mockAxiosInstance();
    attachXServedChainAxiosInterceptor(ax as unknown as never);
    expect(ax.interceptors.response.use).toHaveBeenCalledOnce();
  });

  it("interceptor appends downstream chain to context", () => {
    const ax = mockAxiosInstance();
    attachXServedChainAxiosInterceptor(ax as unknown as never);
    const onFulfilled = ax.interceptors.response.use.mock.calls[0][0];

    runWithChain(() => {
      onFulfilled({
        headers: { "x-served-chain": "inventory-service=canary,audit-service=stable" },
      });
      expect(collectTokens()).toEqual([
        "inventory-service=canary",
        "audit-service=stable",
      ]);
    });
  });

  it("interceptor is a no-op when header absent", () => {
    const ax = mockAxiosInstance();
    attachXServedChainAxiosInterceptor(ax as unknown as never);
    const onFulfilled = ax.interceptors.response.use.mock.calls[0][0];

    runWithChain(() => {
      onFulfilled({ headers: {} });
      expect(collectTokens()).toEqual([]);
    });
  });

  it("interceptor returns the response unchanged", () => {
    const ax = mockAxiosInstance();
    attachXServedChainAxiosInterceptor(ax as unknown as never);
    const onFulfilled = ax.interceptors.response.use.mock.calls[0][0];
    const r = { headers: {}, data: { x: 1 }, status: 200 };
    expect(onFulfilled(r)).toBe(r);
  });
});
