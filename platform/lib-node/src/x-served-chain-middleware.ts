import type { NextFunction, Request, Response } from "express";
import { collectTokens, runWithChain, X_SERVED_CHAIN_HEADER } from "./x-served-chain-context.js";

export function xServedChainMiddleware(): (req: Request, res: Response, next: NextFunction) => void {
  const ownService = (process.env.SERVICE_NAME ?? "").trim() || "unknown";
  const ownVersion = (process.env.VERSION ?? "").trim() || "stable";
  const ownToken = `${ownService}=${ownVersion}`;

  return (_req, res, next) => {
    runWithChain(() => {
      // Patch res.send so the header is set just before the body is sent.
      const originalSend = res.send.bind(res);
      (res as unknown as { send: Response["send"] }).send = ((body?: unknown) => {
        const tokens = [ownToken, ...collectTokens()];
        if (!res.headersSent) {
          res.setHeader(X_SERVED_CHAIN_HEADER, tokens.join(","));
        }
        return originalSend(body);
      }) as Response["send"];

      next();

      // Capture tokens now (still inside the chain context) and emit on finish.
      // This handles unit-test paths that call triggerFinish() without going through send().
      const capturedTokens = [ownToken, ...collectTokens()];
      res.on("finish", () => {
        if (!res.headersSent) {
          res.setHeader(X_SERVED_CHAIN_HEADER, capturedTokens.join(","));
        }
      });
    });
  };
}
