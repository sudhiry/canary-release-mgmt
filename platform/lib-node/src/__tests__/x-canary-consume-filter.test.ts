import { describe, expect, it } from "vitest";
import { shouldProcess, isCanaryFlagged } from "../x-canary-consume-filter.js";

function headers(value: string | undefined): Record<string, Buffer> {
  if (value === undefined) return {};
  return { "x-canary": Buffer.from(value) };
}

describe("isCanaryFlagged", () => {
  it("true for x-canary=true", () => {
    expect(isCanaryFlagged(headers("true"))).toBe(true);
  });
  it("false for x-canary=false", () => {
    expect(isCanaryFlagged(headers("false"))).toBe(false);
  });
  it("false when header absent", () => {
    expect(isCanaryFlagged({})).toBe(false);
    expect(isCanaryFlagged(undefined)).toBe(false);
  });
});

describe("shouldProcess", () => {
  it("canary subset: processes canary-flagged", () => {
    expect(shouldProcess(headers("true"), "canary", () => true)).toBe(true);
  });
  it("canary subset: skips non-canary", () => {
    expect(shouldProcess(headers(undefined), "canary", () => true)).toBe(false);
    expect(shouldProcess(headers("false"), "canary", () => true)).toBe(false);
  });
  it("stable subset: processes non-canary", () => {
    expect(shouldProcess(headers(undefined), "stable", () => true)).toBe(true);
    expect(shouldProcess(headers("false"), "stable", () => true)).toBe(true);
  });
  it("stable subset: skips canary when canary ready", () => {
    expect(shouldProcess(headers("true"), "stable", () => true)).toBe(false);
  });
  it("stable subset: processes canary when canary absent (graceful fallback)", () => {
    expect(shouldProcess(headers("true"), "stable", () => false)).toBe(true);
  });
});
