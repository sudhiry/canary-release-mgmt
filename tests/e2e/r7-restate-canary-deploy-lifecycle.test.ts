import { describe, it, expect } from "vitest";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { listDeployments, listServices } from "./helpers/restate-admin.js";
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

  it("canary teardown: in-flight isolation + stable continues to serve", async () => {
    // This test models the "kill canary while invocations are in flight"
    // scenario that Restate's docs (https://docs.restate.dev/services/versioning)
    // address via pause-resume. β disables pause-resume by construction —
    // *Canary and *Stable are distinct services, so a CheckoutSagaCanary
    // invocation cannot be resumed onto the stable deployment. We assert the
    // structural behavior: in-flight *Canary deployments stay registered with
    // Restate after Helm uninstall (Restate retries dead URLs forever until
    // explicitly DELETE'd from the admin), but new traffic to *Stable services
    // continues to work.

    // Snapshot canary deployment ids before teardown so we can assert their
    // post-uninstall fate.
    const before = await listDeployments();
    const canaryDeploymentIds = before
      .filter((d) =>
        d.services.some((s) => s.name.endsWith("Canary")),
      )
      .map((d) => d.id);
    expect(canaryDeploymentIds.length).toBeGreaterThan(0);

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

    // Restate STILL has the canary deployments registered, even though their
    // URLs are now unreachable. This is the documented β cost: operator must
    // explicitly DELETE the canary deployment(s) from Restate before any
    // future canary install can claim the same URL. R7 surfaces this so
    // operators don't expect automatic cleanup.
    const after = await listDeployments();
    const stillRegistered = after
      .filter((d) => canaryDeploymentIds.includes(d.id))
      .map((d) => d.id);
    expect(
      stillRegistered,
      "β cost: canary deployments remain registered with Restate after Helm uninstall — operator must `restate deployments remove` to clean up",
    ).toEqual(expect.arrayContaining(canaryDeploymentIds));

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
    // To fully clean up before redeploy: `restate deployments remove <id>` for
    // each id in `canaryDeploymentIds` captured above.
  }, 180_000);
});
