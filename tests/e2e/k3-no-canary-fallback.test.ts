import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { sendOrder } from "./helpers/traffic.js";
import { openSubsetForward, waitForConsumed } from "./helpers/consumed-events.js";
import type { PodPortForward } from "./helpers/pod-port-forward.js";
import { ensureCleanBaseline } from "./helpers/cluster.js";

describe("K3 — canary NOT deployed + flagged event → stable falls back", () => {
  let auditStable: PodPortForward;

  beforeAll(async () => {
    await ensureCleanBaseline();
    // Intentionally do NOT deploy any canary subsets.
    auditStable = await openSubsetForward("audit-service", "stable");
  }, 300_000);

  afterAll(async () => {
    await auditStable?.stop();
  });

  it("flagged event lands on stable subset because canary is absent", async () => {
    const r = await sendOrder({ canary: true, user: "k3-fallback-user" });
    expect(r.status).toBeGreaterThanOrEqual(200);
    expect(r.status).toBeLessThan(300);

    const rows = await waitForConsumed(
      auditStable,
      (rows) => rows.some((row) => row.value.includes("k3-fallback-user")),
      15000,
    );
    expect(rows.length).toBeGreaterThan(0);
  });
});
