import { describe, it, expect, beforeAll } from "vitest";
import { sendOrder } from "./helpers/traffic.js";
import { assertServedVersion } from "./helpers/subset.js";
import { ensureCleanBaseline } from "./helpers/cluster.js";

describe("S5 — no-canary graceful fallback", () => {
  beforeAll(async () => { await ensureCleanBaseline(); });

  it("with x-canary header: stable serves (graceful fallback)", async () => {
    const r = await sendOrder({ canary: true, user: "s5-fallback" });
    expect(r.status).toBeGreaterThanOrEqual(200);
    expect(r.status).toBeLessThan(300);
    assertServedVersion(r.headers, "stable");
  });
});
