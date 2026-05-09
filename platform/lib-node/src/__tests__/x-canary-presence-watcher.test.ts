import { describe, expect, it } from "vitest";
import type { V1Pod } from "@kubernetes/client-node";
import { computeCanaryReady, isPodReady } from "../x-canary-presence-watcher.js";

function pod(name: string, ready: boolean | null): V1Pod {
  const p: V1Pod = { metadata: { name } };
  if (ready !== null) {
    p.status = {
      conditions: [{ type: "Ready", status: ready ? "True" : "False" }],
    };
  }
  return p;
}

describe("isPodReady", () => {
  it("true when Ready condition is True", () => {
    expect(isPodReady(pod("p", true))).toBe(true);
  });
  it("false when Ready condition is False", () => {
    expect(isPodReady(pod("p", false))).toBe(false);
  });
  it("false when status missing", () => {
    expect(isPodReady(pod("p", null))).toBe(false);
  });
  it("false when conditions missing", () => {
    const p: V1Pod = { metadata: { name: "p" }, status: {} };
    expect(isPodReady(p)).toBe(false);
  });
});

describe("computeCanaryReady", () => {
  it("false on empty list", () => {
    expect(computeCanaryReady([])).toBe(false);
  });
  it("true when at least one pod is Ready", () => {
    expect(computeCanaryReady([pod("p1", false), pod("p2", true)])).toBe(true);
  });
  it("false when all not Ready", () => {
    expect(computeCanaryReady([pod("p1", false), pod("p2", false)])).toBe(false);
  });
});
