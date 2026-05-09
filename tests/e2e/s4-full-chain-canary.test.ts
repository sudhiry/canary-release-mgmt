import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { deployCanary, rollback } from "./helpers/canary.js";
import { sendOrder } from "./helpers/traffic.js";
import { getChain, assertVersion } from "./helpers/chain.js";
import { ensureCleanBaseline, PHASE1_SERVICES } from "./helpers/cluster.js";

describe("S4 — full-chain canary (all 5)", () => {
  beforeAll(async () => {
    await ensureCleanBaseline();
    for (const svc of PHASE1_SERVICES) {
      await deployCanary(svc, "dev");
    }
  }, 600_000);

  afterAll(async () => {
    for (const svc of PHASE1_SERVICES) {
      await rollback(svc);
    }
  });

  it("header request → every service is canary", async () => {
    const r = await sendOrder({ canary: true, user: "s4-full" });
    expect(r.status).toBeGreaterThanOrEqual(200);
    expect(r.status).toBeLessThan(300);
    const chain = getChain(r.headers);
    for (const svc of PHASE1_SERVICES) {
      assertVersion(chain, svc, "canary");
    }
  });
});
