import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { deployCanary, rollback } from "./helpers/canary.js";
import { sendOrder } from "./helpers/traffic.js";
import { openSubsetForward, getConsumedEvents, waitForConsumed } from "./helpers/consumed-events.js";
import type { PodPortForward } from "./helpers/pod-port-forward.js";
import { ensureCleanBaseline, PHASE1_SERVICES } from "./helpers/cluster.js";

describe("K2 — canary deployed + unflagged event → only stable processes", () => {
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

  it("stable subset records the consumed event; canary subset does not", async () => {
    const r = await sendOrder({ canary: false, user: "k2-stable-user" });
    expect(r.status).toBeGreaterThanOrEqual(200);
    expect(r.status).toBeLessThan(300);

    const stableRows = await waitForConsumed(
      auditStable,
      (rows) => rows.some((row) => row.value.includes("k2-stable-user")),
      15000,
    );
    expect(stableRows.length).toBeGreaterThan(0);

    const canaryRows = await getConsumedEvents(auditCanary);
    expect(canaryRows.some((row) => row.value.includes("k2-stable-user"))).toBe(false);
  });
});
