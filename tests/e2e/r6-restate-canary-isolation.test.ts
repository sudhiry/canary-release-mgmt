import { describe, it, expect } from "vitest";
import { sendOrder } from "./helpers/traffic.js";

describe("R6 — Restate subset isolation under concurrent traffic", () => {
  it("flagged and unflagged concurrent orders each traverse their own subset end-to-end", async () => {
    const N = 10;

    const promises = Array.from({ length: N }, (_, i) => {
      const flagged = i % 2 === 0;
      return sendOrder({
        user: `r6-u${i}`,
        sku: "SKU1",
        quantity: 1,
        amount: 1000,
        canary: flagged,
      }).then((resp) => ({ flagged, resp }));
    });

    const results = await Promise.all(promises);

    for (const { flagged, resp } of results) {
      const variant = flagged ? "canary" : "stable";
      expect(resp.status).toBe(201);

      const order = resp.data as { status?: string; auditTrail?: string[] };
      expect(order.status).toBe("completed");
      expect(order.auditTrail).toEqual([
        `saga@${variant}`,
        `reservation@${variant}`,
        `payment@${variant}`,
        `notification@${variant}`,
      ]);
    }

    // Cross-contamination assertion: no flagged order has any "@stable" entry,
    // and no unflagged order has any "@canary" entry.
    for (const { flagged, resp } of results) {
      const wrong = flagged ? "@stable" : "@canary";
      const order = resp.data as { auditTrail?: string[] };
      if (order.auditTrail) {
        for (const entry of order.auditTrail) {
          expect(entry).not.toContain(wrong);
        }
      }
    }
  }, 60_000);
});
