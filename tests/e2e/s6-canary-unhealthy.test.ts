import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { deployCanary, status, rollback } from "./helpers/canary.js";
import { ensureCleanBaseline } from "./helpers/cluster.js";

describe("S6 — canary unhealthy", () => {
  beforeAll(async () => { await ensureCleanBaseline(); });
  afterAll(async () => { await rollback("payment-service"); });

  it("deploy with bogus image tag auto-rolls back; final state clean", async () => {
    await expect(deployCanary("payment-service", "does-not-exist-bogus-tag-s6"))
      .rejects.toThrow();

    const s = await status("payment-service");
    expect(s.statePhase).toBeNull();
    expect(s.helmCanaryPresent).toBe(false);
    expect(s.vsHasHeaderRule).toBe(false);
    expect(s.drift).toEqual([]);
  });
});
