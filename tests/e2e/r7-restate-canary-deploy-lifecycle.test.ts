import { describe, it, expect } from "vitest";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { listServices } from "./helpers/restate-admin.js";
import { sendOrder } from "./helpers/traffic.js";

const execFileAsync = promisify(execFile);

const RUN = process.env.RUN_CANARY_LIFECYCLE_TESTS === "true";

(RUN ? describe : describe.skip)("R7 — Restate canary deployment lifecycle", () => {
  it("both variant deployments register without conflict", async () => {
    const names = await listServices();
    for (const base of [
      "CheckoutSaga",
      "ReservationWorkflow",
      "PaymentVO",
      "NotificationService",
    ]) {
      expect(names).toContain(`${base}Stable`);
      expect(names).toContain(`${base}Canary`);
    }
  });

  it("per-subset K8s Services have correct selectors", async () => {
    for (const svc of [
      "inventory-service",
      "payment-service",
      "order-service",
      "notification-service",
    ]) {
      for (const variant of ["stable", "canary"] as const) {
        const { stdout } = await execFileAsync(
          "kubectl",
          [
            "-n", "services",
            "get", "svc", `${svc}-${variant}`,
            "-o", "jsonpath={.spec.selector.version}",
          ],
          { encoding: "utf8" },
        );
        expect(stdout.trim()).toBe(variant);
      }
    }
  });

  it("concurrent flagged + unflagged orders maintain isolation (cluster path)", async () => {
    const N = 6;

    const promises = Array.from({ length: N }, (_, i) => {
      const flagged = i % 2 === 0;
      return sendOrder({
        user: `r7-u${i}`,
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
  });

  it("retiring canary leaves stable functional", async () => {
    // Tear down canary releases for each service.
    for (const svc of [
      "inventory-service",
      "payment-service",
      "order-service",
      "notification-service",
    ]) {
      try {
        await execFileAsync("helm", ["uninstall", `${svc}-canary`, "-n", "services"]);
      } catch (e) {
        console.warn(`helm uninstall ${svc}-canary failed (may not exist):`, e);
      }
    }

    // Wait for canary pods to terminate.
    try {
      await execFileAsync(
        "kubectl",
        [
          "wait", "pod",
          "-l", "version=canary",
          "-n", "services",
          "--for=delete",
          "--timeout=60s",
        ],
        { encoding: "utf8" },
      );
    } catch (e) {
      // No canary pods to wait for — that's fine.
    }

    // Issue an unflagged request — stable path should still work.
    const stableResp = await sendOrder({
      user: "r7-retire-stable",
      sku: "SKU1",
      quantity: 1,
      amount: 1000,
      canary: false,
    });
    expect(stableResp.status).toBe(201);

    const order = stableResp.data as { status?: string; auditTrail?: string[] };
    expect(order.status).toBe("completed");
    expect(order.auditTrail).toEqual([
      "saga@stable",
      "reservation@stable",
      "payment@stable",
      "notification@stable",
    ]);

    // Note: this test does not re-install canary. Operator must redeploy after.
  }, 180_000);
});
