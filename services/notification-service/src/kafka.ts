import { Kafka, type Producer, type Consumer, type EachMessagePayload } from "kafkajs";
import { stampXCanaryOnProducerRecord } from "@canary/lib-node";
import { consumedEventStore } from "./store.js";

export interface KafkaSetupOptions {
  brokers: string[];
  consumersEnabled: boolean;
  producerEnabled: boolean;
}

export interface KafkaHandle {
  producer: Producer | null;
  consumer: Consumer | null;
  send: (topic: string, key: string, value: string) => Promise<void>;
}

export async function setupKafka(opts: KafkaSetupOptions): Promise<KafkaHandle> {
  const kafka = new Kafka({ clientId: "notification-service", brokers: opts.brokers });

  let producer: Producer | null = null;
  let send: KafkaHandle["send"];
  if (opts.producerEnabled) {
    producer = kafka.producer();
    await producer.connect();
    const p = producer;
    send = async (topic, key, value) => {
      const record = stampXCanaryOnProducerRecord({
        topic,
        messages: [{ key, value }],
      });
      await p.send(record);
    };
  } else {
    console.log("KAFKA_PRODUCER_ENABLED=false; producer not started; send() is a no-op");
    send = async () => {};
  }

  let consumer: Consumer | null = null;
  if (opts.consumersEnabled) {
    consumer = kafka.consumer({ groupId: "notification-service" });
    await consumer.connect();
    await consumer.subscribe({ topics: ["orders.events", "payments.events"] });
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
    console.log("notification-service Kafka consumer subscribed to orders.events, payments.events");
  } else {
    console.log("KAFKA_CONSUMERS_ENABLED=false; consumer not started");
  }

  return { producer, consumer, send };
}
