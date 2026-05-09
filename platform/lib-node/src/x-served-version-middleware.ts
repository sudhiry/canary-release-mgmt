import type { NextFunction, Request, Response } from "express";

export const X_SERVED_VERSION_HEADER = "x-served-version";
export const X_SERVED_VERSION_DEFAULT = "stable";

/**
 * Returns an Express middleware that stamps `x-served-version: <VERSION>` on
 * every response. The version is captured once at factory call time from
 * `process.env.VERSION`, defaulting to "stable" if unset or blank.
 */
export function xServedVersionMiddleware(): (req: Request, res: Response, next: NextFunction) => void {
  const raw = process.env.VERSION;
  const version = raw && raw.trim().length > 0 ? raw.trim() : X_SERVED_VERSION_DEFAULT;
  return (_req, res, next) => {
    res.setHeader(X_SERVED_VERSION_HEADER, version);
    next();
  };
}
