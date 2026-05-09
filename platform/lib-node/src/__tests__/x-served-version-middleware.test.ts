import { describe, expect, it, vi, afterEach } from "vitest";
import type { Request, Response, NextFunction } from "express";
import { xServedVersionMiddleware, X_SERVED_VERSION_HEADER } from "../x-served-version-middleware.js";

function mockRes(): Response {
  return { setHeader: vi.fn() } as unknown as Response;
}

describe("xServedVersionMiddleware", () => {
  const originalVersion = process.env.VERSION;

  afterEach(() => {
    if (originalVersion === undefined) delete process.env.VERSION;
    else process.env.VERSION = originalVersion;
  });

  it("sets the header to VERSION env var when set", () => {
    process.env.VERSION = "canary";
    const mw = xServedVersionMiddleware();
    const res = mockRes();
    const next = vi.fn() as unknown as NextFunction;
    mw({} as Request, res, next);
    expect(res.setHeader).toHaveBeenCalledWith("x-served-version", "canary");
    expect(next).toHaveBeenCalled();
  });

  it("defaults to stable when VERSION is unset", () => {
    delete process.env.VERSION;
    const mw = xServedVersionMiddleware();
    const res = mockRes();
    const next = vi.fn() as unknown as NextFunction;
    mw({} as Request, res, next);
    expect(res.setHeader).toHaveBeenCalledWith("x-served-version", "stable");
  });

  it("defaults to stable when VERSION is empty string", () => {
    process.env.VERSION = "";
    const mw = xServedVersionMiddleware();
    const res = mockRes();
    const next = vi.fn() as unknown as NextFunction;
    mw({} as Request, res, next);
    expect(res.setHeader).toHaveBeenCalledWith("x-served-version", "stable");
  });

  it("captures the version once at factory call (does not re-read env per request)", () => {
    process.env.VERSION = "stable";
    const mw = xServedVersionMiddleware();
    process.env.VERSION = "canary"; // mutate AFTER factory call
    const res = mockRes();
    const next = vi.fn() as unknown as NextFunction;
    mw({} as Request, res, next);
    expect(res.setHeader).toHaveBeenCalledWith("x-served-version", "stable");
  });

  it("exports X_SERVED_VERSION_HEADER constant", () => {
    expect(X_SERVED_VERSION_HEADER).toBe("x-served-version");
  });
});
