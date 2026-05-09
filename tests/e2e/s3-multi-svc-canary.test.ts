import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { deployCanary, rollback } from "./helpers/canary.js";
import { sendOrder } from "./helpers/traffic.js";
import { getChain, assertVersions } from "./helpers/chain.js";
import { ensureCleanBaseline } from "./helpers/cluster.js";

describe("S3 — multi-service canary (order + inventory)", () => {
  beforeAll(async () => {
    await ensureCleanBaseline();
    await deployCanary("order-service", "dev");
    await deployCanary("inventory-service", "dev");
  }, 240_000);

  afterAll(async () => {
    await rollback("order-service");
    await rollback("inventory-service");
  });

  it("header request → order=canary, inventory=canary, others=stable", async () => {
    const r = await sendOrder({ canary: true, user: "s3-multi" });
    expect(r.status).toBeGreaterThanOrEqual(200);
    expect(r.status).toBeLessThan(300);
    const chain = getChain(r.headers);
    assertVersions(chain, {
      "order-service": "canary",
      "inventory-service": "canary",
      "payment-service": "stable",
      "notification-service": "stable",
      "audit-service": "stable",
    });
  });
});
