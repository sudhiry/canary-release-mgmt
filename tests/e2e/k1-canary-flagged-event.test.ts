import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { deployCanary, rollback } from "./helpers/canary.js";
import { sendOrder } from "./helpers/traffic.js";
import { openSubsetForward, getConsumedEvents, waitForConsumed } from "./helpers/consumed-events.js";
import type { PodPortForward } from "./helpers/pod-port-forward.js";
import { ensureCleanBaseline, PHASE1_SERVICES } from "./helpers/cluster.js";

describe("K1 — canary deployed + flagged event → only canary processes", () => {
  let auditStable: PodPortForward;
  let auditCanary: PodPortForward;

  beforeAll(async () => {
    await ensureCleanBaseline();
    for (const svc of PHASE1_SERVICES) {
      await deployCanary(svc, "dev");
    }
    auditStable = await openSubsetForward("audit-service", "stable");
    auditCanary = await openSubsetForward("audit-service", "canary");
  }, 600_000);

  afterAll(async () => {
    await auditStable?.stop();
    await auditCanary?.stop();
    for (const svc of PHASE1_SERVICES) {
      await rollback(svc);
    }
  });

  it("canary subset records the consumed event; stable subset does not", async () => {
    const r = await sendOrder({ canary: true, user: "k1-canary-user" });
    expect(r.status).toBeGreaterThanOrEqual(200);
    expect(r.status).toBeLessThan(300);

    const canaryRows = await waitForConsumed(
      auditCanary,
      (rows) => rows.some((row) => row.value.includes("k1-canary-user")),
      15000,
    );
    expect(canaryRows.some((row) => row.headers["x-canary"] === "true")).toBe(true);

    const stableRows = await getConsumedEvents(auditStable);
    expect(stableRows.some((row) => row.value.includes("k1-canary-user"))).toBe(false);
  });
});
