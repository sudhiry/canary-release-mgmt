import type { NextFunction, Request, Response } from "express";
import { runWithCanary } from "./x-canary-context.js";
import { X_CANARY_HEADER, X_CANARY_TRUE } from "./x-canary-constants.js";

export function xCanaryMiddleware(req: Request, _res: Response, next: NextFunction): void {
  const headerValue = req.headers[X_CANARY_HEADER];
  const value = Array.isArray(headerValue) ? headerValue[0] : headerValue;
  const canary = value === X_CANARY_TRUE;
  // Use runWithCanary so the entire request handler runs inside the context frame.
  void runWithCanary(canary, async () => {
    next();
  });
}
