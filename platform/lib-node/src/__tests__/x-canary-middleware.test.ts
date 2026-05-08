import { describe, it, expect, afterEach } from "vitest";
import { isCanary, clearCanary } from "../x-canary-context.js";
import { xCanaryMiddleware } from "../x-canary-middleware.js";

function fakeReq(headers: Record<string, string | undefined>): any {
  return { headers, get: (name: string) => headers[name.toLowerCase()] };
}

describe("xCanaryMiddleware", () => {
  afterEach(() => clearCanary());

  it("sets the context to true when x-canary header is 'true'", () => {
    return new Promise<void>((resolve) => {
      const req = fakeReq({ "x-canary": "true" });
      const next = () => {
        expect(isCanary()).toBe(true);
        resolve();
      };
      xCanaryMiddleware(req as any, {} as any, next as any);
    });
  });

  it("leaves the context as false when header is absent", () => {
    return new Promise<void>((resolve) => {
      const req = fakeReq({});
      const next = () => {
        expect(isCanary()).toBe(false);
        resolve();
      };
      xCanaryMiddleware(req as any, {} as any, next as any);
    });
  });

  it("leaves the context as false for non-'true' header values", () => {
    return new Promise<void>((resolve) => {
      const req = fakeReq({ "x-canary": "yes" });
      const next = () => {
        expect(isCanary()).toBe(false);
        resolve();
      };
      xCanaryMiddleware(req as any, {} as any, next as any);
    });
  });
});
