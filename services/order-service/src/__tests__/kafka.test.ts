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

describe("setupKafka (order-service)", () => {
  beforeEach(() => {
    connectMock.mockClear();
    sendMock.mockClear();
    subscribeMock.mockClear();
    runMock.mockClear();
    consumerConnectMock.mockClear();
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
    expect(subscribeMock).toHaveBeenCalledWith({
      topics: ["payments.events", "inventory.events"],
    });
    expect(runMock).toHaveBeenCalledOnce();
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
});
