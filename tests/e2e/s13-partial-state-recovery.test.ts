import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { deployCanary, rollback, status, reconcile } from "./helpers/canary.js";
import { ensureCleanBaseline } from "./helpers/cluster.js";

const execFileAsync = promisify(execFile);

describe("S13 — canary-ctl partial-state recovery", () => {
  beforeAll(async () => { await ensureCleanBaseline(); });
  afterAll(async () => { await rollback("payment-service"); });

  it("manually deleted VS header rule is restored by reconcile", async () => {
    await deployCanary("payment-service", "dev");
    let s = await status("payment-service");
    expect(s.vsHasHeaderRule).toBe(true);

    const defaultOnlyPatch = JSON.stringify({
      spec: { http: [{ name: "default", route: [{ destination: { host: "payment-service", subset: "stable" } }] }] },
    });
    await execFileAsync("kubectl", [
      "patch", "virtualservice", "payment-service",
      "-n", "services",
      "--type", "merge",
      "-p", defaultOnlyPatch,
    ]);

    s = await status("payment-service");
    expect(s.drift.length).toBeGreaterThan(0);

    await reconcile("payment-service");

    s = await status("payment-service");
    expect(s.vsHasHeaderRule).toBe(true);
    expect(s.drift).toEqual([]);
  });
});
