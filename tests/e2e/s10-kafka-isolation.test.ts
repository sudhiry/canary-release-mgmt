import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { deployCanary, rollback } from "./helpers/canary.js";
import { connect as kafkaConnect, disconnect as kafkaDisconnect, listConsumerGroups } from "./helpers/kafka-admin.js";
import { startKafkaPortForward, type KafkaPortForward } from "./helpers/kafka-port-forward.js";
import { ensureCleanBaseline } from "./helpers/cluster.js";

let pf: KafkaPortForward | null = null;

describe("S10 — Kafka isolation (canary does not subscribe)", () => {
  beforeAll(async () => {
    await ensureCleanBaseline();
    pf = await startKafkaPortForward();
    await kafkaConnect();
    await deployCanary("order-service", "dev");
  }, 180_000);

  afterAll(async () => {
    await rollback("order-service");
    await kafkaDisconnect();
    if (pf) await pf.stop();
  });

  it("Kafka consumer groups contain no canary pod members", async () => {
    const groups = await listConsumerGroups();
    const canaryGroups = groups.filter((g) => /canary/i.test(g));
    expect(canaryGroups).toEqual([]);
  });
});
