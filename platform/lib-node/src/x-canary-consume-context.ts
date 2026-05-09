import { runWithCanary } from "./x-canary-context.js";
import { isCanaryFlagged, type KafkaConsumeHeaders } from "./x-canary-consume-filter.js";

export async function runWithCanaryFromHeaders<T>(
  headers: KafkaConsumeHeaders,
  handler: () => Promise<T>,
): Promise<T> {
  const canary = isCanaryFlagged(headers);
  return runWithCanary(canary, handler);
}
