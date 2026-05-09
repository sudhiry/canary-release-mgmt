import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { deployCanary, rollback } from "./helpers/canary.js";
import { sendOrder } from "./helpers/traffic.js";
import { getChain, assertVersion } from "./helpers/chain.js";
import { ensureCleanBaseline } from "./helpers/cluster.js";

describe("S2 — single-service canary (payment-service)", () => {
  beforeAll(async () => {
    await ensureCleanBaseline();
    await deployCanary("payment-service", "dev");
  }, 180_000);

  afterAll(async () => { await rollback("payment-service"); });

  it("header request → payment is canary; others are stable", async () => {
    const r = await sendOrder({ canary: true, user: "s2-canary" });
    expect(r.status).toBeGreaterThanOrEqual(200);
    expect(r.status).toBeLessThan(300);
    const chain = getChain(r.headers);
    assertVersion(chain, "order-service", "stable");
    assertVersion(chain, "payment-service", "canary");
    assertVersion(chain, "inventory-service", "stable");
    assertVersion(chain, "notification-service", "stable");
    assertVersion(chain, "audit-service", "stable");
  });

  it("no-header request → all stable", async () => {
    const r = await sendOrder({ canary: false, user: "s2-stable" });
    expect(r.status).toBeGreaterThanOrEqual(200);
    expect(r.status).toBeLessThan(300);
    const chain = getChain(r.headers);
    assertVersion(chain, "payment-service", "stable");
  });
});
