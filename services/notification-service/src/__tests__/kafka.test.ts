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

describe("setupKafka", () => {
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

    await kafka.send("notifications.events", "n_1", "{}");
    expect(sendMock).not.toHaveBeenCalled();
  });

  it("does NOT call consumer.subscribe / consumer.run when consumersEnabled=false", async () => {
    await setupKafka({ brokers: ["localhost:9092"], consumersEnabled: false, producerEnabled: true });
    expect(subscribeMock).not.toHaveBeenCalled();
    expect(runMock).not.toHaveBeenCalled();
  });

  it("subscribes to orders.events + payments.events when consumersEnabled=true", async () => {
    await setupKafka({ brokers: ["localhost:9092"], consumersEnabled: true, producerEnabled: true });
    // Consumer setup runs in the background; wait for it to reach subscribe+run.
    await vi.waitFor(() => {
      expect(subscribeMock).toHaveBeenCalledWith({
        topics: ["orders.events", "payments.events"],
      });
      expect(runMock).toHaveBeenCalledOnce();
    });
  });

  it("send() wraps records via stampXCanaryOnProducerRecord (calls producer.send)", async () => {
    const kafka = await setupKafka({ brokers: ["localhost:9092"], consumersEnabled: false, producerEnabled: true });
    await kafka.send("notifications.events", "n_1", "{}");

    expect(sendMock).toHaveBeenCalledOnce();
    const arg = sendMock.mock.calls[0][0] as { topic: string; messages: Array<{ key: string; value: string }> };
    expect(arg.topic).toBe("notifications.events");
    expect(arg.messages[0].key).toBe("n_1");
    expect(arg.messages[0].value).toBe("{}");
    // Without canary context, headers should be absent or empty.
  });

  it("setupKafka() returns immediately even when producer.connect() never resolves", async () => {
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

      await expect(kafka.send("notifications.events", "n_1", "{}")).resolves.toBeUndefined();
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

  it("uses resolveConsumerGroupId('notification-service') for the group id", async () => {
    // With VERSION=canary the resolved groupId must be 'notification-service-canary'.
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
    expect(consumerCreatedWithMock.mock.calls[0][0].groupId).toBe("notification-service-canary");
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
      topic: "orders.events",
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
      topic: "orders.events",
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
    await eachMessage({ topic: "orders.events", partition: 0, message: { headers: {}, key: Buffer.from("k1"), value: Buffer.from("{}") } });
    await eachMessage({ topic: "payments.events", partition: 0, message: { headers: {}, key: Buffer.from("k2"), value: Buffer.from("{}") } });
    await eachMessage({ topic: "orders.events", partition: 0, message: { headers: { "x-canary": Buffer.from("true") }, key: Buffer.from("k3"), value: Buffer.from("{}") } });

    expect(recordPollSpy).toHaveBeenCalledTimes(3);
  });
});
