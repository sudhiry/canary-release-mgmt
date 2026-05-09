import { describe, expect, it } from "vitest";
import { runWithCanaryFromHeaders } from "../x-canary-consume-context.js";
import { isCanary } from "../x-canary-context.js";

function headers(value: string | undefined): Record<string, Buffer> {
  if (value === undefined) return {};
  return { "x-canary": Buffer.from(value) };
}

describe("runWithCanaryFromHeaders", () => {
  it("runs handler in canary context when header is true", async () => {
    let observed = false;
    await runWithCanaryFromHeaders(headers("true"), async () => {
      observed = isCanary();
    });
    expect(observed).toBe(true);
  });

  it("runs handler in stable context when header is absent", async () => {
    let observed = true;
    await runWithCanaryFromHeaders(headers(undefined), async () => {
      observed = isCanary();
    });
    expect(observed).toBe(false);
  });

  it("propagates handler return value", async () => {
    const r = await runWithCanaryFromHeaders(headers("true"), async () => 42);
    expect(r).toBe(42);
  });

  it("propagates handler exceptions", async () => {
    await expect(
      runWithCanaryFromHeaders(headers("true"), async () => { throw new Error("boom"); }),
    ).rejects.toThrow(/boom/);
  });
});
