import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const connectMock = vi.fn().mockResolvedValue(undefined);
const sendMock = vi.fn().mockResolvedValue(undefined);
const subscribeMock = vi.fn().mockResolvedValue(undefined);
const runMock = vi.fn().mockResolvedValue(undefined);
const consumerConnectMock = vi.fn().mockResolvedValue(undefined);

// Track which groupId was passed to kafka.consumer() so tests can assert on it.
const consumerCreatedWithMock = vi.fn();

vi.mock("kafkajs", () => {
  return {
    Kafka: vi.fn().mockImplementation(() => ({
      producer: () => ({ connect: connectMock, send: sendMock }),
      consumer: (cfg: { groupId: string }) => {
        consumerCreatedWithMock(cfg);
        return {
          connect: consumerConnectMock,
          subscribe: subscribeMock,
          run: runMock,
        };
      },
    })),
  };
});

import { setupKafka } from "../kafka.js";
import { consumedEventStore } from "../store.js";
import request from "supertest";
import { setupHttp } from "../http.js";
import type { SagaClients } from "../saga.js";
import type { AxiosInstance } from "axios";

// Access the private events array for clearing between tests (store has no .clear() method).
function clearConsumedEvents() {
  (consumedEventStore as unknown as { events: unknown[] }).events = [];
}

// Helper: build a fake EachMessagePayload message for driving the consumer.
type FakeMessage = {
  headers: Record<string, Buffer>;
  key: Buffer | null;
  value: Buffer | null;
};
type FakePayload = { topic: string; partition: number; message: FakeMessage };
type EachMessageFn = (payload: FakePayload) => Promise<void>;

/** Wait for runMock to be called and return the captured eachMessage handler. */
async function captureEachMessage(): Promise<EachMessageFn> {
  await vi.waitFor(() => {
    expect(runMock).toHaveBeenCalledOnce();
  });
  return (runMock.mock.calls[0][0] as { eachMessage: EachMessageFn }).eachMessage;
}

describe("setupKafka (order-service)", () => {
  beforeEach(() => {
    connectMock.mockClear();
    sendMock.mockClear();
    subscribeMock.mockClear();
    runMock.mockClear();
    consumerConnectMock.mockClear();
    consumerCreatedWithMock.mockClear();
  });

  it("connects producer when producerEnabled=true", async () => {
    await setupKafka({ brokers: ["localhost:9092"], consumersEnabled: false, producerEnabled: true });
    expect(connectMock).toHaveBeenCalledOnce();
  });

  it("does NOT connect producer when producerEnabled=false; send() is a no-op", async () => {
    const kafka = await setupKafka({ brokers: ["localhost:9092"], consumersEnabled: false, producerEnabled: false });
    expect(connectMock).not.toHaveBeenCalled();
    expect(kafka.producer).toBeNull();

    await kafka.send("orders.events", "ord_1", "{}");
    expect(sendMock).not.toHaveBeenCalled();
  });

  it("does NOT subscribe / run consumer when consumersEnabled=false", async () => {
    await setupKafka({ brokers: ["localhost:9092"], consumersEnabled: false, producerEnabled: true });
    expect(subscribeMock).not.toHaveBeenCalled();
    expect(runMock).not.toHaveBeenCalled();
  });

  it("subscribes to payments.events + inventory.events when consumersEnabled=true", async () => {
    await setupKafka({ brokers: ["localhost:9092"], consumersEnabled: true, producerEnabled: true });
    // Consumer setup runs in the background; wait for it to reach subscribe+run.
    await vi.waitFor(() => {
      expect(subscribeMock).toHaveBeenCalledWith({
        topics: ["payments.events", "inventory.events"],
      });
      expect(runMock).toHaveBeenCalledOnce();
    });
  });

  it("send() forwards orders.events through producer.send (with stamping wrapper applied)", async () => {
    const kafka = await setupKafka({ brokers: ["localhost:9092"], consumersEnabled: false, producerEnabled: true });
    await kafka.send("orders.events", "ord_1", "{}");

    expect(sendMock).toHaveBeenCalledOnce();
    const arg = sendMock.mock.calls[0][0];
    expect(arg.topic).toBe("orders.events");
    expect(arg.messages[0].key).toBe("ord_1");
    expect(arg.messages[0].value).toBe("{}");
  });

  it("setupKafka() returns immediately even when producer.connect() never resolves", async () => {
    // simulate a broker that never answers — connect() pending forever
    connectMock.mockImplementationOnce(() => new Promise<void>(() => {}));

    const start = Date.now();
    const kafka = await setupKafka({
      brokers: ["localhost:9092"],
      consumersEnabled: false,
      producerEnabled: true,
      sendTimeoutMs: 50,
      reconnectIntervalMs: 50,
    });
    const elapsed = Date.now() - start;

    expect(elapsed).toBeLessThan(200);
    expect(kafka.producer).not.toBeNull();
  });

  it("send() drops event with warning when producer not connected within sendTimeoutMs", async () => {
    connectMock.mockImplementationOnce(() => new Promise<void>(() => {}));
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      const kafka = await setupKafka({
        brokers: ["localhost:9092"],
        consumersEnabled: false,
        producerEnabled: true,
        sendTimeoutMs: 50,
        reconnectIntervalMs: 50,
      });

      await expect(kafka.send("orders.events", "ord_1", "{}")).resolves.toBeUndefined();
      expect(sendMock).not.toHaveBeenCalled();
      expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining("send dropped"));
    } finally {
      warnSpy.mockRestore();
    }
  });
});

describe("setupKafka — Phase 2.b canary integration", () => {
  const origVersion = process.env.VERSION;

  beforeEach(() => {
    connectMock.mockClear();
    sendMock.mockClear();
    subscribeMock.mockClear();
    runMock.mockClear();
    consumerConnectMock.mockClear();
    consumerCreatedWithMock.mockClear();
    clearConsumedEvents();
    process.env.VERSION = "stable";
  });

  afterEach(() => {
    if (origVersion === undefined) {
      delete process.env.VERSION;
    } else {
      process.env.VERSION = origVersion;
    }
  });

  it("uses resolveConsumerGroupId('order-service') for the group id", async () => {
    // With VERSION=canary the resolved groupId must be 'order-service-canary'.
    process.env.VERSION = "canary";
    await setupKafka({
      brokers: ["localhost:9092"],
      consumersEnabled: true,
      producerEnabled: false,
      presenceWatcherEnabled: false,
    });

    await vi.waitFor(() => {
      expect(consumerCreatedWithMock).toHaveBeenCalledOnce();
    });
    expect(consumerCreatedWithMock.mock.calls[0][0].groupId).toBe("order-service-canary");
  });

  it("filter: stable + canaryReady=true skips x-canary messages", async () => {
    // Stable pod; canary is ready (_testIsCanaryReady=()=>true injected) →
    // must NOT process canary-flagged messages.
    process.env.VERSION = "stable";

    await setupKafka({
      brokers: ["localhost:9092"],
      consumersEnabled: true,
      producerEnabled: false,
      presenceWatcherEnabled: false,
      _testIsCanaryReady: () => true,
    });

    const eachMessage = await captureEachMessage();

    await eachMessage({
      topic: "payments.events",
      partition: 0,
      message: {
        headers: { "x-canary": Buffer.from("true") },
        key: Buffer.from("k1"),
        value: Buffer.from('{"id":"1"}'),
      },
    });

    expect(consumedEventStore.all()).toHaveLength(0);
  });

  it("filter: canary processes x-canary messages", async () => {
    process.env.VERSION = "canary";

    await setupKafka({
      brokers: ["localhost:9092"],
      consumersEnabled: true,
      producerEnabled: false,
      presenceWatcherEnabled: false,
    });

    const eachMessage = await captureEachMessage();

    await eachMessage({
      topic: "payments.events",
      partition: 0,
      message: {
        headers: { "x-canary": Buffer.from("true") },
        key: Buffer.from("k1"),
        value: Buffer.from('{"id":"1"}'),
      },
    });

    expect(consumedEventStore.all()).toHaveLength(1);
    expect(consumedEventStore.all()[0].headers["x-canary"]).toBe("true");
  });

  it("filter: canary skips messages without x-canary header", async () => {
    process.env.VERSION = "canary";

    await setupKafka({
      brokers: ["localhost:9092"],
      consumersEnabled: true,
      producerEnabled: false,
      presenceWatcherEnabled: false,
    });

    const eachMessage = await captureEachMessage();

    await eachMessage({
      topic: "payments.events",
      partition: 0,
      message: {
        headers: {},
        key: Buffer.from("k2"),
        value: Buffer.from('{"id":"2"}'),
      },
    });

    expect(consumedEventStore.all()).toHaveLength(0);
  });

  it("recordPoll is called for each message regardless of filter result", async () => {
    // Use canary pod: it skips non-canary messages, so we get both filtered and processed paths.
    process.env.VERSION = "canary";

    const kafkaHandle = await setupKafka({
      brokers: ["localhost:9092"],
      consumersEnabled: true,
      producerEnabled: false,
      presenceWatcherEnabled: false,
    });

    const eachMessage = await captureEachMessage();
    const recordPollSpy = vi.spyOn(kafkaHandle.health, "recordPoll");

    // Drive 3 messages: two without x-canary (filtered out), one with x-canary (processed).
    await eachMessage({ topic: "payments.events", partition: 0, message: { headers: {}, key: Buffer.from("k1"), value: Buffer.from("{}") } });
    await eachMessage({ topic: "payments.events", partition: 0, message: { headers: {}, key: Buffer.from("k2"), value: Buffer.from("{}") } });
    await eachMessage({ topic: "payments.events", partition: 0, message: { headers: { "x-canary": Buffer.from("true") }, key: Buffer.from("k3"), value: Buffer.from("{}") } });

    expect(recordPollSpy).toHaveBeenCalledTimes(3);
  });

  it("/health on CANARY returns 503 when kafka health state is stale", async () => {
    const { createKafkaHealthState } = await import("@canary/lib-node");
    // Create a health state with 1ms timeout, record a poll, then let it go stale.
    const staleHealth = createKafkaHealthState(1);
    staleHealth.recordPoll();
    await new Promise<void>((r) => setTimeout(r, 10));

    const mockClients: SagaClients = {
      inventory: { post: vi.fn() } as unknown as AxiosInstance,
      payment: { post: vi.fn() } as unknown as AxiosInstance,
      notification: { post: vi.fn() } as unknown as AxiosInstance,
    };

    const app = setupHttp({ clients: mockClients, kafkaHealth: staleHealth, version: "canary" });
    const res = await request(app).get("/health");
    expect(res.status).toBe(503);
    expect(res.body.ok).toBe(false);
  });
});
