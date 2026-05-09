import { describe, expect, it, vi, afterEach } from "vitest";
import type { Request, Response, NextFunction } from "express";
import {
  xServedChainMiddleware,
} from "../x-served-chain-middleware.js";
import { appendToken } from "../x-served-chain-context.js";

interface MockResponse extends Response {
  triggerFinish: () => void;
  capturedHeaders: Record<string, string>;
}

function mockRes(): MockResponse {
  let onFinishCb: (() => void) | undefined;
  const headers: Record<string, string> = {};
  const res = {
    setHeader: vi.fn((k: string, v: string) => { headers[k] = v; }),
    getHeader: vi.fn((k: string) => headers[k]),
    on: vi.fn((event: string, cb: () => void) => {
      if (event === "finish") onFinishCb = cb;
    }),
    send: vi.fn(function(this: Response, _body?: unknown) { return this; }),
    headersSent: false,
  } as unknown as MockResponse;
  res.triggerFinish = () => onFinishCb?.();
  res.capturedHeaders = headers;
  return res;
}

describe("xServedChainMiddleware", () => {
  const original = { svc: process.env.SERVICE_NAME, ver: process.env.VERSION };
  afterEach(() => {
    if (original.svc === undefined) delete process.env.SERVICE_NAME; else process.env.SERVICE_NAME = original.svc;
    if (original.ver === undefined) delete process.env.VERSION; else process.env.VERSION = original.ver;
  });

  it("emits own token only when no downstream tokens", () => {
    process.env.SERVICE_NAME = "payment-service";
    process.env.VERSION = "stable";
    const mw = xServedChainMiddleware();
    const res = mockRes();
    const next = vi.fn(() => {
      // No downstream calls — context stays empty.
    }) as unknown as NextFunction;
    mw({} as Request, res, next);
    res.triggerFinish();
    expect(res.capturedHeaders["x-served-chain"]).toBe("payment-service=stable");
  });

  it("prepends own token to downstream chain", () => {
    process.env.SERVICE_NAME = "order-service";
    process.env.VERSION = "canary";
    const mw = xServedChainMiddleware();
    const res = mockRes();
    const next: NextFunction = () => {
      appendToken("inventory-service=canary");
      appendToken("audit-service=stable");
    };
    mw({} as Request, res, next);
    res.triggerFinish();
    expect(res.capturedHeaders["x-served-chain"]).toBe(
      "order-service=canary,inventory-service=canary,audit-service=stable",
    );
  });

  it("defaults to unknown=stable when env unset", () => {
    delete process.env.SERVICE_NAME;
    delete process.env.VERSION;
    const mw = xServedChainMiddleware();
    const res = mockRes();
    const next: NextFunction = () => {};
    mw({} as Request, res, next);
    res.triggerFinish();
    expect(res.capturedHeaders["x-served-chain"]).toBe("unknown=stable");
  });
});
