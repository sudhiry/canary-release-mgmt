import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { deployCanary, rollback } from "./helpers/canary.js";
import { listDeployments } from "./helpers/restate-admin.js";
import { ensureCleanBaseline } from "./helpers/cluster.js";

describe("S11 — Restate isolation (canary does not register handlers)", () => {
  beforeAll(async () => { await ensureCleanBaseline(); });
  afterAll(async () => { await rollback("payment-service"); });

  it("after canary deploy, Restate registry contains stable services only", async () => {
    await deployCanary("payment-service", "dev");

    await new Promise((r) => setTimeout(r, 5000));

    const deployments = await listDeployments();
    for (const d of deployments) {
      if (d.uri && /-canary/.test(d.uri)) {
        throw new Error(`Restate registered a canary deployment: ${d.uri}`);
      }
    }
  });
});
