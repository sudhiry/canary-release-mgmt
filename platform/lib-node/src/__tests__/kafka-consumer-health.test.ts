import { describe, expect, it } from "vitest";
import { createKafkaHealthState } from "../kafka-consumer-health.js";

describe("kafka-consumer-health", () => {
  it("initially not healthy (no poll yet)", () => {
    const s = createKafkaHealthState(30_000);
    expect(s.isHealthy()).toBe(false);
    expect(s.report().ok).toBe(false);
    expect(s.report().reason).toMatch(/no poll/i);
  });

  it("healthy after recordPoll", () => {
    const s = createKafkaHealthState(30_000);
    s.recordPoll();
    expect(s.isHealthy()).toBe(true);
    expect(s.report().ok).toBe(true);
  });

  it("not healthy when stale beyond timeout", async () => {
    const s = createKafkaHealthState(50);
    s.recordPoll();
    await new Promise((r) => setTimeout(r, 100));
    expect(s.isHealthy()).toBe(false);
    expect(s.report().reason).toMatch(/stale/i);
  });
});
