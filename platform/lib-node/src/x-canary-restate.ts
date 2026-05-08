import { isCanary } from "./x-canary-context.js";
import { X_CANARY_HEADER, X_CANARY_TRUE } from "./x-canary-constants.js";
import type { ClientCallOptions } from "@restatedev/restate-sdk";

/**
 * Applies x-canary: true to the `headers` field of a Restate per-call options
 * object when the current async context is canary.
 *
 * Targets `ClientCallOptions` (and the compatible `ClientSendOptions`) from
 * @restatedev/restate-sdk@1.14.2, both of which expose
 * `headers?: Record<string, string>`.
 *
 * Usage:
 *   ctx.serviceClient<MyService>(MyService).myMethod(
 *     input,
 *     applyXCanaryToRestateOptions({ headers: {} })
 *   );
 */
export function applyXCanaryToRestateOptions<
  T extends Pick<ClientCallOptions<unknown, unknown>, "headers">,
>(options: T): T {
  if (!isCanary()) {
    return options;
  }
  const headers = { ...(options.headers ?? {}) };
  if (headers[X_CANARY_HEADER] === undefined) {
    headers[X_CANARY_HEADER] = X_CANARY_TRUE;
  }
  return { ...options, headers };
}
