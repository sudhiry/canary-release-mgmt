import { X_CANARY_HEADER, X_CANARY_TRUE } from "./x-canary-constants.js";

export type KafkaConsumeHeaders = Record<string, Buffer | undefined> | undefined;

export function isCanaryFlagged(headers: KafkaConsumeHeaders): boolean {
  if (!headers) return false;
  const raw = headers[X_CANARY_HEADER];
  if (!raw) return false;
  return raw.toString("utf8") === X_CANARY_TRUE;
}

export function shouldProcess(
  headers: KafkaConsumeHeaders,
  ownVersion: string,
  isCanaryReady: () => boolean,
): boolean {
  const carriesCanary = isCanaryFlagged(headers);
  if (ownVersion === "canary") {
    return carriesCanary;
  }
  return !carriesCanary || !isCanaryReady();
}
