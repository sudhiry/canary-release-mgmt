import { describe, it, expect, beforeAll } from "vitest";
import { sendOrder } from "./helpers/traffic.js";
import { assertServedVersion } from "./helpers/subset.js";
import { status } from "./helpers/canary.js";

const SERVICES = [
  "order-service",
  "payment-service",
  "inventory-service",
  "notification-service",
  "audit-service",
] as const;

describe("S1 Baseline — all stable, no canaries deployed", () => {
  beforeAll(async () => {
    // Verify the cluster is in the all-stable starting state.
    for (const svc of SERVICES) {
      const s = await status(svc);
      if (s.helmCanaryPresent) {
        throw new Error(
          `Pre-condition failed: ${svc} has a canary release. Run \`make canary-rollback SVC=${svc}\` first.`,
        );
      }
      if (s.vsHasHeaderRule) {
        throw new Error(
          `Pre-condition failed: ${svc} VS has a header rule. Run \`make canary-rollback SVC=${svc}\` first.`,
        );
      }
    }
  });

  it("GET /api/orders without x-canary returns 2xx from stable", async () => {
    const r = await sendOrder({ canary: false, user: "s1-stable" });
    expect(r.status).toBeGreaterThanOrEqual(200);
    expect(r.status).toBeLessThan(300);
    assertServedVersion(r.headers, "stable");
  });

  it("GET /api/orders with x-canary returns 2xx from stable (graceful fallback)", async () => {
    const r = await sendOrder({ canary: true, user: "s1-fallback" });
    expect(r.status).toBeGreaterThanOrEqual(200);
    expect(r.status).toBeLessThan(300);
    assertServedVersion(r.headers, "stable");
  });
});
