import type { CanaryMetrics } from "./canary-metrics.js";

/**
 * Wraps a Restate handler body with timer + outcome counter emission.
 * Records `success` if the body resolves, `server_error` if it throws.
 * Re-throws on error; non-Error throws are also classified server_error.
 */
export async function measureRestate<T>(
  metrics: CanaryMetrics,
  handlerName: string,
  body: () => Promise<T>,
): Promise<T> {
  const startNs = process.hrtime.bigint();
  try {
    const result = await body();
    const elapsed = Number(process.hrtime.bigint() - startNs) / 1e9;
    metrics.recordRestate(handlerName, "success", elapsed);
    return result;
  } catch (err) {
    const elapsed = Number(process.hrtime.bigint() - startNs) / 1e9;
    metrics.recordRestate(handlerName, "server_error", elapsed);
    throw err;
  }
}
