import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { deployCanary, rollback } from "./helpers/canary.js";
import { runLoad, type LoadStats } from "./helpers/load.js";
import { ensureCleanBaseline } from "./helpers/cluster.js";

const PAYLOAD = { userId: "s7-load", sku: "sku-1", quantity: 1, amount: 100 };
const HEADERS = { "content-type": "application/json" };
const URL = "http://localhost:8080/api/orders";
const RPS = 50;
const DURATION = 30;
const TOLERANCE = 1.5;

describe("S7 — stable not disrupted by canary deploy", () => {
  let baseline: LoadStats;

  beforeAll(async () => {
    await ensureCleanBaseline();
    baseline = await runLoad({
      url: URL, method: "POST", rps: RPS, durationSeconds: DURATION,
      headers: HEADERS, payload: PAYLOAD,
    });
    expect(baseline.failureCount).toBe(0);
  }, 90_000);

  afterAll(async () => { await rollback("payment-service"); });

  it(`p99 stable load during canary deploy stays within ${TOLERANCE}x baseline`, async () => {
    await deployCanary("payment-service", "dev");

    const during = await runLoad({
      url: URL, method: "POST", rps: RPS, durationSeconds: DURATION,
      headers: HEADERS, payload: PAYLOAD,
    });

    expect(during.failureCount).toBe(0);
    expect(during.p99Ms).toBeLessThanOrEqual(baseline.p99Ms * TOLERANCE);
  }, 120_000);
});
