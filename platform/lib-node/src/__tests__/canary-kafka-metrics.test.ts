import type { Consumer, EachMessagePayload, KafkaMessage } from "kafkajs";
import { Registry } from "prom-client";
import { describe, expect, it, vi } from "vitest";
import { CanaryMetrics } from "../observability/canary-metrics.js";
import { wrapKafkaConsumer } from "../observability/canary-kafka-metrics.js";

function fakeMessage(): KafkaMessage {
  return {
    key: Buffer.from("k"),
    value: Buffer.from("v"),
    timestamp: "0",
    size: 1,
    attributes: 0,
    offset: "0",
  };
}

function fakePayload(topic: string): EachMessagePayload {
  return {
    topic,
    partition: 0,
    message: fakeMessage(),
    heartbeat: async () => {},
    pause: () => () => {},
  };
}

describe("wrapKafkaConsumer", () => {
  it("records success when eachMessage resolves", async () => {
    const registry = new Registry();
    const metrics = new CanaryMetrics("audit", registry);
    const eachMessage = vi.fn().mockResolvedValue(undefined);

    let captured: ((p: EachMessagePayload) => Promise<void>) | undefined;
    const consumer = {
      run: vi.fn(async (cfg: { eachMessage?: (p: EachMessagePayload) => Promise<void> }) => {
        captured = cfg.eachMessage;
      }),
    } as unknown as Consumer;

    const wrapped = wrapKafkaConsumer(consumer, metrics);
    await wrapped.run({ eachMessage });

    await captured!(fakePayload("payments.charged"));
    expect(eachMessage).toHaveBeenCalled();

    const c = await registry.getSingleMetric("canary_request_total")?.get();
    expect(c?.values).toContainEqual(
      expect.objectContaining({
        value: 1,
        labels: expect.objectContaining({
          substrate: "kafka",
          outcome: "success",
          target: "payments.charged",
        }),
      }),
    );
  });

  it("records server_error and re-throws when eachMessage throws", async () => {
    const registry = new Registry();
    const metrics = new CanaryMetrics("audit", registry);
    const eachMessage = vi.fn().mockRejectedValue(new Error("boom"));

    let captured: ((p: EachMessagePayload) => Promise<void>) | undefined;
    const consumer = {
      run: vi.fn(async (cfg: { eachMessage?: (p: EachMessagePayload) => Promise<void> }) => {
        captured = cfg.eachMessage;
      }),
    } as unknown as Consumer;

    const wrapped = wrapKafkaConsumer(consumer, metrics);
    await wrapped.run({ eachMessage });

    await expect(captured!(fakePayload("payments.charged"))).rejects.toThrow("boom");

    const c = await registry.getSingleMetric("canary_request_total")?.get();
    expect(c?.values).toContainEqual(
      expect.objectContaining({
        value: 1,
        labels: expect.objectContaining({ outcome: "server_error" }),
      }),
    );
  });
});
