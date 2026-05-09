export type ServedVersion = "stable" | "canary";

export const X_SERVED_VERSION_HEADER = "x-served-version";

export function getServedVersion(headers: Record<string, string | string[] | undefined>): ServedVersion | null {
  const raw = headers[X_SERVED_VERSION_HEADER];
  const value = Array.isArray(raw) ? raw[0] : raw;
  if (value === "stable" || value === "canary") return value;
  return null;
}

export function assertServedVersion(
  headers: Record<string, string | string[] | undefined>,
  expected: ServedVersion,
): void {
  const got = getServedVersion(headers);
  if (got === null) {
    const headerNames = Object.keys(headers).join(", ");
    throw new Error(
      `x-served-version header missing or unrecognized. Headers received: [${headerNames}]`,
    );
  }
  if (got !== expected) {
    throw new Error(`x-served-version: expected ${expected}, got ${got}`);
  }
}
