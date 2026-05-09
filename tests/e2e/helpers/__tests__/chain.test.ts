import { describe, expect, it } from "vitest";
import {
  parseChain,
  getChain,
  assertVersion,
  assertVersions,
  assertContains,
  assertAbsent,
  type ChainEntry,
} from "../chain.js";

describe("chain helpers", () => {
  it("parseChain splits into entries", () => {
    const r = parseChain("order-service=canary,inventory-service=canary,audit-service=stable");
    expect(r).toEqual([
      { service: "order-service", version: "canary" },
      { service: "inventory-service", version: "canary" },
      { service: "audit-service", version: "stable" },
    ] satisfies ChainEntry[]);
  });

  it("parseChain skips malformed tokens", () => {
    const r = parseChain("a=b,no-equals,c=d");
    expect(r).toEqual([
      { service: "a", version: "b" },
      { service: "c", version: "d" },
    ]);
  });

  it("parseChain returns empty on empty/null", () => {
    expect(parseChain(undefined)).toEqual([]);
    expect(parseChain("")).toEqual([]);
    expect(parseChain("   ")).toEqual([]);
  });

  it("getChain reads the header from a response headers object", () => {
    expect(getChain({ "x-served-chain": "a=b" })).toEqual([
      { service: "a", version: "b" },
    ]);
    expect(getChain({})).toEqual([]);
  });

  it("assertVersion succeeds when service has expected version", () => {
    const c = parseChain("order-service=canary,payment-service=stable");
    expect(() => assertVersion(c, "order-service", "canary")).not.toThrow();
    expect(() => assertVersion(c, "payment-service", "stable")).not.toThrow();
  });

  it("assertVersion throws when version mismatches", () => {
    const c = parseChain("order-service=stable");
    expect(() => assertVersion(c, "order-service", "canary"))
      .toThrow(/order-service: expected canary, got stable/);
  });

  it("assertVersion throws when service absent", () => {
    const c = parseChain("order-service=stable");
    expect(() => assertVersion(c, "payment-service", "stable"))
      .toThrow(/payment-service: not present in chain/);
  });

  it("assertVersions checks multiple services at once", () => {
    const c = parseChain("order-service=canary,payment-service=stable,audit-service=stable");
    expect(() => assertVersions(c, {
      "order-service": "canary",
      "payment-service": "stable",
    })).not.toThrow();
  });

  it("assertContains succeeds when service present at any version", () => {
    const c = parseChain("order-service=canary");
    expect(() => assertContains(c, "order-service")).not.toThrow();
    expect(() => assertContains(c, "audit-service")).toThrow(/not present/);
  });

  it("assertAbsent succeeds when service NOT in chain", () => {
    const c = parseChain("order-service=canary");
    expect(() => assertAbsent(c, "payment-service")).not.toThrow();
    expect(() => assertAbsent(c, "order-service")).toThrow(/unexpectedly present/);
  });
});
