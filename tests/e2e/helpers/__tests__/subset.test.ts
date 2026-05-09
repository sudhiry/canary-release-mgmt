import { describe, expect, it } from "vitest";
import { assertServedVersion, getServedVersion } from "../subset.js";

describe("subset helpers", () => {
  it("getServedVersion returns the header value when present", () => {
    expect(getServedVersion({ "x-served-version": "stable" })).toBe("stable");
    expect(getServedVersion({ "x-served-version": "canary" })).toBe("canary");
  });

  it("getServedVersion returns null when header absent", () => {
    expect(getServedVersion({})).toBeNull();
    expect(getServedVersion({ "other-header": "value" })).toBeNull();
  });

  it("getServedVersion returns null for unrecognized values", () => {
    expect(getServedVersion({ "x-served-version": "v2" })).toBeNull();
  });

  it("assertServedVersion succeeds when header matches expected", () => {
    expect(() => assertServedVersion({ "x-served-version": "stable" }, "stable")).not.toThrow();
    expect(() => assertServedVersion({ "x-served-version": "canary" }, "canary")).not.toThrow();
  });

  it("assertServedVersion throws clear error when header missing", () => {
    expect(() => assertServedVersion({}, "stable")).toThrow(/x-served-version header missing/i);
  });

  it("assertServedVersion throws clear error when header mismatches", () => {
    expect(() => assertServedVersion({ "x-served-version": "stable" }, "canary"))
      .toThrow(/expected canary, got stable/i);
  });
});
