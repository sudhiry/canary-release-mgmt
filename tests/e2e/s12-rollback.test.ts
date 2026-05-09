import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { deployCanary, rollback, status } from "./helpers/canary.js";
import { sendOrder } from "./helpers/traffic.js";
import { assertServedVersion } from "./helpers/subset.js";
import { ensureCleanBaseline } from "./helpers/cluster.js";

describe("S12 — rollback", () => {
  beforeAll(async () => { await ensureCleanBaseline(); });
  afterAll(async () => { await rollback("payment-service"); });

  it("deploy then rollback leaves cluster clean; subsequent header request → stable", async () => {
    await deployCanary("payment-service", "dev");
    let s = await status("payment-service");
    expect(s.statePhase).toBe("active");
    expect(s.vsHasHeaderRule).toBe(true);

    await rollback("payment-service");
    s = await status("payment-service");
    expect(s.statePhase).toBeNull();
    expect(s.helmCanaryPresent).toBe(false);
    expect(s.vsHasHeaderRule).toBe(false);
    expect(s.drift).toEqual([]);

    const r = await sendOrder({ canary: true, user: "s12-after-rollback" });
    expect(r.status).toBeGreaterThanOrEqual(200);
    expect(r.status).toBeLessThan(300);
    assertServedVersion(r.headers, "stable");
  });
});
