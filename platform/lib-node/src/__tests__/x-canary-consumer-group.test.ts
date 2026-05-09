import { describe, expect, it, afterEach } from "vitest";
import { resolveConsumerGroupId } from "../x-canary-consumer-group.js";

describe("resolveConsumerGroupId", () => {
  const original = process.env.VERSION;

  afterEach(() => {
    if (original === undefined) delete process.env.VERSION;
    else process.env.VERSION = original;
  });

  it("appends -stable when VERSION env is stable", () => {
    process.env.VERSION = "stable";
    expect(resolveConsumerGroupId("orders-events")).toBe("orders-events-stable");
  });

  it("appends -canary when VERSION env is canary", () => {
    process.env.VERSION = "canary";
    expect(resolveConsumerGroupId("orders-events")).toBe("orders-events-canary");
  });

  it("defaults to -stable when VERSION env is unset", () => {
    delete process.env.VERSION;
    expect(resolveConsumerGroupId("base")).toBe("base-stable");
  });

  it("defaults to -stable when VERSION is blank", () => {
    process.env.VERSION = "   ";
    expect(resolveConsumerGroupId("base")).toBe("base-stable");
  });

  it("throws on blank base", () => {
    expect(() => resolveConsumerGroupId("")).toThrow(/base.*blank/i);
    expect(() => resolveConsumerGroupId("   ")).toThrow(/base.*blank/i);
  });
});
