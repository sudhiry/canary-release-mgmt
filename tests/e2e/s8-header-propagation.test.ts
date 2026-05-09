import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { deployCanary, rollback } from "./helpers/canary.js";
import { sendOrder } from "./helpers/traffic.js";
import { getChain, assertContains } from "./helpers/chain.js";
import { ensureCleanBaseline, PHASE1_SERVICES } from "./helpers/cluster.js";

describe("S8 — header propagation completeness", () => {
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

  it("chain contains all 5 services (every internal hop reached)", async () => {
    const r = await sendOrder({ canary: true, user: "s8-propagation" });
    expect(r.status).toBeGreaterThanOrEqual(200);
    expect(r.status).toBeLessThan(300);
    const chain = getChain(r.headers);
    for (const svc of PHASE1_SERVICES) {
      assertContains(chain, svc);
    }
    const auditCount = chain.filter((e) => e.service === "audit-service").length;
    expect(auditCount).toBeGreaterThanOrEqual(1);
  });
});
