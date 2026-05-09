import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { deployCanary, rollback } from "./helpers/canary.js";
import { sendOrder } from "./helpers/traffic.js";
import { openSubsetForward, waitForConsumed } from "./helpers/consumed-events.js";
import { findPodByLabel, sendSignalToPod, type PodPortForward } from "./helpers/pod-port-forward.js";
import { ensureCleanBaseline } from "./helpers/cluster.js";

describe("K5 — canary Kafka unhealthy → stable takes over flagged events", () => {
  let canaryPod = "";
  let auditStable: PodPortForward;
  let auditCanary: PodPortForward | null = null;

  beforeAll(async () => {
    await ensureCleanBaseline();
    await deployCanary("audit-service", "dev");
    canaryPod = await findPodByLabel("services", "app=audit-service,version=canary");
    auditStable = await openSubsetForward("audit-service", "stable");
    auditCanary = await openSubsetForward("audit-service", "canary");
  }, 300_000);

  afterAll(async () => {
    // Best-effort revive in case the test left the pod in STOP state.
    if (canaryPod) { try { await sendSignalToPod("services", canaryPod, "CONT"); } catch {} }
    await auditStable?.stop();
    await auditCanary?.stop().catch(() => {});
    await rollback("audit-service");
  });

  it("after SIGSTOP on canary, stable processes a flagged event", async () => {
    // Step A: baseline — flagged event goes to canary while canary is healthy
    const baseline = await sendOrder({ canary: true, user: "k5-baseline" });
    expect(baseline.status).toBeGreaterThanOrEqual(200);
    expect(baseline.status).toBeLessThan(300);
    await waitForConsumed(
      auditCanary!,
      (rows) => rows.some((r) => r.value.includes("k5-baseline")),
      15000,
    );

    // Step B: stop the canary process — readiness will fail within KAFKA_HEALTH_TIMEOUT_MS,
    // then kubelet probe failureThreshold × periodSeconds, then watch propagation.
    await sendSignalToPod("services", canaryPod, "STOP");
    // The canary forward will hang once SIGSTOP'd; close it so vitest doesn't deadlock.
    await auditCanary!.stop().catch(() => {});
    auditCanary = null;

    // Default kafka health timeout 30s + probe failureThreshold 3 × periodSeconds 5 = 15s
    // + watch propagation ~1s. Allow up to 60s.
    await new Promise((r) => setTimeout(r, 60_000));

    // Step C: send a flagged event — should now land on stable
    const flagged = await sendOrder({ canary: true, user: "k5-fallback" });
    expect(flagged.status).toBeGreaterThanOrEqual(200);
    expect(flagged.status).toBeLessThan(300);

    const stableRows = await waitForConsumed(
      auditStable,
      (rows) => rows.some((r) => r.value.includes("k5-fallback")),
      30000,
    );
    expect(stableRows.some((r) => r.value.includes("k5-fallback"))).toBe(true);

    // Step D: revive canary so the cluster returns to a clean state
    await sendSignalToPod("services", canaryPod, "CONT");
  }, 180_000);
});
