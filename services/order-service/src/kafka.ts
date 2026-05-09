import { Kafka, type Producer, type Consumer, type EachMessagePayload } from "kafkajs";
import { stampXCanaryOnProducerRecord } from "@canary/lib-node";
import { consumedEventStore } from "./store.js";

export interface KafkaSetupOptions {
  brokers: string[];
  consumersEnabled: boolean;
}

export interface KafkaHandle {
  producer: Producer;
  consumer: Consumer | null;
  send: (topic: string, key: string, value: string) => Promise<void>;
}

export async function setupKafka(opts: KafkaSetupOptions): Promise<KafkaHandle> {
  const kafka = new Kafka({ clientId: "order-service", brokers: opts.brokers });

  const producer = kafka.producer();
  await producer.connect();

  const send = async (topic: string, key: string, value: string): Promise<void> => {
    const record = stampXCanaryOnProducerRecord({
      topic,
      messages: [{ key, value }],
    });
    await producer.send(record);
  };

  let consumer: Consumer | null = null;
  if (opts.consumersEnabled) {
    consumer = kafka.consumer({ groupId: "order-service" });
    await consumer.connect();
    await consumer.subscribe({ topics: ["payments.events", "inventory.events"] });
    await consumer.run({
      eachMessage: async ({ topic, message }: EachMessagePayload) => {
        const headers: Record<string, string> = {};
        for (const [k, v] of Object.entries(message.headers ?? {})) {
          if (v) headers[k] = Buffer.isBuffer(v) ? v.toString("utf8") : String(v);
        }
        consumedEventStore.record({
          topic,
          key: message.key?.toString("utf8") ?? null,
          value: message.value?.toString("utf8") ?? "",
          headers,
        });
      },
    });
    console.log("order-service Kafka consumer subscribed to payments.events, inventory.events");
  } else {
    console.log("KAFKA_CONSUMERS_ENABLED=false; consumer not started");
  }

  return { producer, consumer, send };
}
