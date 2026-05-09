import { describe, expect, it } from "vitest";
import {
  appendToken,
  appendChain,
  collectTokens,
  runWithChain,
} from "../x-served-chain-context.js";

describe("xServedChainContext", () => {
  it("collectTokens returns empty when no tokens appended", () => {
    runWithChain(() => {
      expect(collectTokens()).toEqual([]);
    });
  });

  it("appendToken accumulates in order", () => {
    runWithChain(() => {
      appendToken("inventory-service=canary");
      appendToken("audit-service=stable");
      expect(collectTokens()).toEqual([
        "inventory-service=canary",
        "audit-service=stable",
      ]);
    });
  });

  it("appendChain splits CSV", () => {
    runWithChain(() => {
      appendChain("a=1,b=2,c=3");
      expect(collectTokens()).toEqual(["a=1", "b=2", "c=3"]);
    });
  });

  it("appendChain ignores blank/null", () => {
    runWithChain(() => {
      appendChain(undefined);
      appendChain("");
      appendChain("   ");
      expect(collectTokens()).toEqual([]);
    });
  });

  it("contexts are isolated per runWithChain frame", async () => {
    const results: string[][] = [];
    await Promise.all([
      runWithChain(async () => {
        appendToken("frame-a=v1");
        await new Promise((r) => setTimeout(r, 10));
        results.push([...collectTokens()]);
      }),
      runWithChain(async () => {
        appendToken("frame-b=v1");
        results.push([...collectTokens()]);
      }),
    ]);
    expect(results.sort()).toEqual([["frame-a=v1"], ["frame-b=v1"]].sort());
  });

  it("appendToken outside runWithChain is a no-op", () => {
    appendToken("ignored=x");
    expect(collectTokens()).toEqual([]);
  });
});
