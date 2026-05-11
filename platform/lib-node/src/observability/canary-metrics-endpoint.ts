import type { Request, Response } from "express";
import type { CanaryMetrics } from "./canary-metrics.js";

/** Express handler exposing the prom-client registry's text-format output. */
export function canaryMetricsEndpoint(metrics: CanaryMetrics) {
  const registry = metrics.getRegistry();
  return async function (_req: Request, res: Response): Promise<void> {
    res.set("Content-Type", registry.contentType);
    res.status(200).send(await registry.metrics());
  };
}
