import { describe, expect, it } from "vitest";
import { createKafkaHealthState } from "../kafka-consumer-health.js";

describe("kafka-consumer-health", () => {
  it("not assigned → unhealthy", () => {
    const s = createKafkaHealthState(15_000);
    expect(s.isHealthy()).toBe(false);
    expect(s.report().ok).toBe(false);
    expect(s.report().reason).toMatch(/no partitions assigned/i);
  });

  it("assigned but no heartbeat → unhealthy", () => {
    const s = createKafkaHealthState(15_000);
    s.markAssigned();
    expect(s.isHealthy()).toBe(false);
    expect(s.report().reason).toMatch(/no heartbeat/i);
  });

  it("assigned + fresh heartbeat → healthy", () => {
    const s = createKafkaHealthState(15_000);
    s.markAssigned();
    s.recordHeartbeat();
    expect(s.isHealthy()).toBe(true);
    expect(s.report().ok).toBe(true);
  });

  it("assigned + stale heartbeat → unhealthy", async () => {
    const s = createKafkaHealthState(50);
    s.markAssigned();
    s.recordHeartbeat();
    await new Promise((r) => setTimeout(r, 100));
    expect(s.isHealthy()).toBe(false);
    expect(s.report().reason).toMatch(/stale/i);
  });

  it("revoked after assigned → unhealthy", () => {
    const s = createKafkaHealthState(15_000);
    s.markAssigned();
    s.recordHeartbeat();
    expect(s.isHealthy()).toBe(true);
    s.markRevoked();
    expect(s.isHealthy()).toBe(false);
  });
});
