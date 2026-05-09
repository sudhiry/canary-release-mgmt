import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { deployCanary, rollback } from "./helpers/canary.js";
import { sendOrder } from "./helpers/traffic.js";
import { openSubsetForward, waitForConsumed } from "./helpers/consumed-events.js";
import type { PodPortForward } from "./helpers/pod-port-forward.js";
import { ensureCleanBaseline, PHASE1_SERVICES } from "./helpers/cluster.js";

describe("K4 — Kafka consume context propagates x-canary downstream", () => {
  let auditCanary: PodPortForward;
  let orderCanary: PodPortForward;

  beforeAll(async () => {
    await ensureCleanBaseline();
    for (const svc of PHASE1_SERVICES) {
      await deployCanary(svc, "dev");
    }
    auditCanary = await openSubsetForward("audit-service", "canary");
    orderCanary = await openSubsetForward("order-service", "canary");
  }, 600_000);

  afterAll(async () => {
    await auditCanary?.stop();
    await orderCanary?.stop();
    for (const svc of PHASE1_SERVICES) {
      await rollback(svc);
    }
  });

  it("canary's audit-service downstream events carry x-canary=true", async () => {
    const r = await sendOrder({ canary: true, user: "k4-propagation-user" });
    expect(r.status).toBeGreaterThanOrEqual(200);
    expect(r.status).toBeLessThan(300);

    const auditRows = await waitForConsumed(
      auditCanary,
      (rows) => rows.some((row) => row.value.includes("k4-propagation-user")),
      15000,
    );
    const matchedAudit = auditRows.find((r) => r.value.includes("k4-propagation-user"));
    expect(matchedAudit?.headers["x-canary"]).toBe("true");

    // The downstream events the chain emits (e.g., payments.events, inventory.events)
    // must carry x-canary=true at their canary consumers. order-service canary consumes
    // payments.events + inventory.events; verify the header is present there.
    const orderRows = await waitForConsumed(
      orderCanary,
      (rows) => rows.some((row) => row.value.includes("k4-propagation-user")),
      15000,
    );
    const matchedOrder = orderRows.find((r) => r.value.includes("k4-propagation-user"));
    expect(matchedOrder?.headers["x-canary"]).toBe("true");
  });
});
