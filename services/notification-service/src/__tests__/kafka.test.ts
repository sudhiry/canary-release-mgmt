import { describe, it, expect, vi, beforeEach } from "vitest";

const connectMock = vi.fn().mockResolvedValue(undefined);
const sendMock = vi.fn().mockResolvedValue(undefined);
const subscribeMock = vi.fn().mockResolvedValue(undefined);
const runMock = vi.fn().mockResolvedValue(undefined);
const consumerConnectMock = vi.fn().mockResolvedValue(undefined);

vi.mock("kafkajs", () => {
  return {
    Kafka: vi.fn().mockImplementation(() => ({
      producer: () => ({ connect: connectMock, send: sendMock }),
      consumer: () => ({
        connect: consumerConnectMock,
        subscribe: subscribeMock,
        run: runMock,
      }),
    })),
  };
});

import { setupKafka } from "../kafka.js";

describe("setupKafka", () => {
  beforeEach(() => {
    connectMock.mockClear();
    sendMock.mockClear();
    subscribeMock.mockClear();
    runMock.mockClear();
    consumerConnectMock.mockClear();
  });

  it("connects producer always", async () => {
    await setupKafka({ brokers: ["localhost:9092"], consumersEnabled: false });
    expect(connectMock).toHaveBeenCalledOnce();
  });

  it("does NOT call consumer.subscribe / consumer.run when consumersEnabled=false", async () => {
    await setupKafka({ brokers: ["localhost:9092"], consumersEnabled: false });
    expect(subscribeMock).not.toHaveBeenCalled();
    expect(runMock).not.toHaveBeenCalled();
  });

  it("subscribes to orders.events + payments.events when consumersEnabled=true", async () => {
    await setupKafka({ brokers: ["localhost:9092"], consumersEnabled: true });
    expect(subscribeMock).toHaveBeenCalledWith({
      topics: ["orders.events", "payments.events"],
    });
    expect(runMock).toHaveBeenCalledOnce();
  });

  it("send() wraps records via stampXCanaryOnProducerRecord (calls producer.send)", async () => {
    const kafka = await setupKafka({ brokers: ["localhost:9092"], consumersEnabled: false });
    await kafka.send("notifications.events", "n_1", "{}");

    expect(sendMock).toHaveBeenCalledOnce();
    const arg = sendMock.mock.calls[0][0] as { topic: string; messages: Array<{ key: string; value: string }> };
    expect(arg.topic).toBe("notifications.events");
    expect(arg.messages[0].key).toBe("n_1");
    expect(arg.messages[0].value).toBe("{}");
    // Without canary context, headers should be absent or empty.
  });
});
