import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const connectMock = vi.fn().mockResolvedValue(undefined);
const sendMock = vi.fn().mockResolvedValue(undefined);
const subscribeMock = vi.fn().mockResolvedValue(undefined);
const runMock = vi.fn().mockResolvedValue(undefined);
const consumerConnectMock = vi.fn().mockResolvedValue(undefined);

// Track which groupId was passed to kafka.consumer() so tests can assert on it.
const consumerCreatedWithMock = vi.fn();

// Static event-name table mirroring kafkajs ConsumerEvents.
const consumerEvents = {
  HEARTBEAT: "consumer.heartbeat",
  COMMIT_OFFSETS: "consumer.commit_offsets",
  GROUP_JOIN: "consumer.group_join",
  FETCH_START: "consumer.fetch_start",
  FETCH: "consumer.fetch",
  START_BATCH_PROCESS: "consumer.start_batch_process",
  END_BATCH_PROCESS: "consumer.end_batch_process",
  CONNECT: "consumer.connect",
  DISCONNECT: "consumer.disconnect",
  STOP: "consumer.stop",
  CRASH: "consumer.crash",
  REBALANCING: "consumer.rebalancing",
  RECEIVED_UNSUBSCRIBED_TOPICS: "consumer.received_unsubscribed_topics",
  REQUEST: "consumer.network.request",
  REQUEST_TIMEOUT: "consumer.network.request_timeout",
  REQUEST_QUEUE_SIZE: "consumer.network.request_queue_size",
} as const;

// Track event handlers registered on the most-recently-created consumer so tests
// can fire kafkajs-style events (GROUP_JOIN, HEARTBEAT, REBALANCING, DISCONNECT)
// against the wiring set up by setupKafka.
type Handlers = Map<string, Array<(payload: unknown) => void>>;
let lastConsumerHandlers: Handlers | null = null;

function triggerConsumerEvent(eventName: string, payload: unknown = {}): void {
  if (!lastConsumerHandlers) {
    throw new Error("no consumer created yet — call setupKafka first");
  }
  const fns = lastConsumerHandlers.get(eventName) ?? [];
  for (const fn of fns) fn(payload);
}

vi.mock("kafkajs", () => {
  return {
    Kafka: vi.fn().mockImplementation(() => ({
      producer: () => ({ connect: connectMock, send: sendMock }),
      consumer: (cfg: { groupId: string }) => {
        consumerCreatedWithMock(cfg);
        const handlers: Handlers = new Map();
        lastConsumerHandlers = handlers;
        return {
          connect: consumerConnectMock,
          subscribe: subscribeMock,
          run: runMock,
          events: consumerEvents,
          on(eventName: string, handler: (payload: unknown) => void) {
            const list = handlers.get(eventName) ?? [];
            list.push(handler);
            handlers.set(eventName, list);
            return () => {
              const cur = handlers.get(eventName);
              if (!cur) return;
              const idx = cur.indexOf(handler);
              if (idx >= 0) cur.splice(idx, 1);
            };
          },
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

  it("wires kafkajs consumer events into KafkaHealthState (group_join/rebalancing/heartbeat/disconnect)", async () => {
    process.env.VERSION = "canary";

    const kafkaHandle = await setupKafka({
      brokers: ["localhost:9092"],
      consumersEnabled: true,
      producerEnabled: false,
      presenceWatcherEnabled: false,
    });

    // Wait for setupKafka's background consumer setup to register handlers.
    await vi.waitFor(() => {
      expect(lastConsumerHandlers?.get("consumer.group_join")?.length ?? 0).toBeGreaterThan(0);
      expect(lastConsumerHandlers?.get("consumer.heartbeat")?.length ?? 0).toBeGreaterThan(0);
      expect(lastConsumerHandlers?.get("consumer.rebalancing")?.length ?? 0).toBeGreaterThan(0);
      expect(lastConsumerHandlers?.get("consumer.disconnect")?.length ?? 0).toBeGreaterThan(0);
    });

    const markAssignedSpy = vi.spyOn(kafkaHandle.health, "markAssigned");
    const markRevokedSpy = vi.spyOn(kafkaHandle.health, "markRevoked");
    const recordHeartbeatSpy = vi.spyOn(kafkaHandle.health, "recordHeartbeat");

    triggerConsumerEvent("consumer.group_join");
    triggerConsumerEvent("consumer.heartbeat");
    triggerConsumerEvent("consumer.heartbeat");
    triggerConsumerEvent("consumer.rebalancing");
    triggerConsumerEvent("consumer.disconnect");

    expect(markAssignedSpy).toHaveBeenCalledTimes(1);
    expect(recordHeartbeatSpy).toHaveBeenCalledTimes(2);
    // REBALANCING + DISCONNECT both call markRevoked.
    expect(markRevokedSpy).toHaveBeenCalledTimes(2);

    // After GROUP_JOIN + HEARTBEAT + (eventually) REBALANCING + DISCONNECT, the
    // state machine should be unhealthy (no partitions assigned).
    expect(kafkaHandle.health.isHealthy()).toBe(false);
  });

  it("/health on CANARY returns 503 when kafka health state is stale", async () => {
    const { createKafkaHealthState } = await import("@canary/lib-node");
    // Create a health state with 1ms timeout, mark assigned + record heartbeat, then let it go stale.
    const staleHealth = createKafkaHealthState(1);
    staleHealth.markAssigned();
    staleHealth.recordHeartbeat();
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
