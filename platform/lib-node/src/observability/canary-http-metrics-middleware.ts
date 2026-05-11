import type { NextFunction, Request, Response } from "express";
import type { CanaryMetrics, Outcome } from "./canary-metrics.js";

/**
 * Express middleware that records canary HTTP metrics on response finish.
 * Must be registered AFTER xCanaryMiddleware so XCanaryContext is populated.
 */
export function canaryHttpMetricsMiddleware(metrics: CanaryMetrics) {
  return function metricsMiddleware(req: Request, res: Response, next: NextFunction): void {
    const startNs = process.hrtime.bigint();

    res.on("finish", () => {
      const elapsedSeconds = Number(process.hrtime.bigint() - startNs) / 1e9;
      const outcome = classifyStatus(res.statusCode);
      const target = `${req.method} ${routeOf(req)}`;
      metrics.recordHttp(target, outcome, elapsedSeconds);
    });

    next();
  };
}

function classifyStatus(status: number): Outcome {
  if (status >= 500) return "server_error";
  if (status >= 400) return "client_error";
  return "success";
}

function routeOf(req: Request): string {
  // Prefer matched route pattern (low cardinality) over raw URL.
  // After routing, Express sets req.route. Before routing — fall back to path.
  // Cast through any to bypass narrow Express type.
  const route = (req as unknown as { route?: { path?: string } }).route;
  return route?.path ?? req.path;
}
