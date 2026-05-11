import { isCanary } from "../x-canary-context.js";

export type Lane = "stable" | "canary";

export const STABLE: Lane = "stable";
export const CANARY: Lane = "canary";

/** Returns the current lane derived from XCanaryContext. */
export function currentLane(): Lane {
  return isCanary() ? CANARY : STABLE;
}
