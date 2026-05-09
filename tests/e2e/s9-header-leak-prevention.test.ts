import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { randomUUID } from "node:crypto";
import { deployCanary, rollback } from "./helpers/canary.js";
import { sendOrder } from "./helpers/traffic.js";
import { getPodLogs, logsContain } from "./helpers/pod-logs.js";
import { ensureCleanBaseline } from "./helpers/cluster.js";

describe("S9 — header leak prevention", () => {
  beforeAll(async () => {
    await ensureCleanBaseline();
    await deployCanary("payment-service", "dev");
  }, 180_000);

  afterAll(async () => { await rollback("payment-service"); });

  it("no-header request does NOT reach canary pod logs", async () => {
    const uniqueUserId = `s9-leak-${randomUUID()}`;
    const r = await sendOrder({ canary: false, user: uniqueUserId });
    expect(r.status).toBeGreaterThanOrEqual(200);
    expect(r.status).toBeLessThan(300);

    await new Promise((r) => setTimeout(r, 3000));

    const logs = await getPodLogs({
      namespace: "services",
      labelSelector: "app=payment-service,version=canary",
      sinceSeconds: 30,
    });
    expect(logsContain(logs, uniqueUserId)).toBe(false);
  });
});
