import { Kafka, type Producer, type Consumer, type EachMessagePayload } from "kafkajs";
import { stampXCanaryOnProducerRecord } from "@canary/lib-node";
import { consumedEventStore } from "./store.js";

export interface KafkaSetupOptions {
  brokers: string[];
  consumersEnabled: boolean;
  producerEnabled: boolean;
  /**
   * How long send() waits for the producer to finish connecting before
   * dropping the event with a warning. Default: 5000ms.
   */
  sendTimeoutMs?: number;
  /**
   * How long the background connect loop waits between retries after
   * KafkaJS's internal retry budget is exhausted. Default: 10000ms.
   */
  reconnectIntervalMs?: number;
}

export interface KafkaHandle {
  producer: Producer | null;
  consumer: Consumer | null;
  send: (topic: string, key: string, value: string) => Promise<void>;
}

export async function setupKafka(opts: KafkaSetupOptions): Promise<KafkaHandle> {
  const kafka = new Kafka({ clientId: "order-service", brokers: opts.brokers });
  const sendTimeoutMs = opts.sendTimeoutMs ?? 5000;
  const reconnectIntervalMs = opts.reconnectIntervalMs ?? 10000;

  let producer: Producer | null = null;
  let send: KafkaHandle["send"];
  if (opts.producerEnabled) {
    producer = kafka.producer();
    const p = producer;
    let connected = false;

    // Connect in the background so app.listen() can proceed without waiting on
    // Kafka availability. KafkaJS's internal retry handles transient errors
    // during a single connect(); the outer loop handles longer outages.
    const connectPromise = (async () => {
      while (true) {
        try {
          await p.connect();
          connected = true;
          console.log("order-service Kafka producer connected");
          return;
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          console.warn(`order-service Kafka producer connect failed: ${msg}; retrying in ${reconnectIntervalMs}ms`);
          await new Promise((r) => setTimeout(r, reconnectIntervalMs));
        }
      }
    })();
    connectPromise.catch(() => {});

    send = async (topic, key, value) => {
      if (!connected) {
        const result = await Promise.race([
          connectPromise.then(() => "ready" as const),
          new Promise<"timeout">((r) => setTimeout(() => r("timeout"), sendTimeoutMs)),
        ]);
        if (result === "timeout") {
          console.warn(
            `order-service kafka send dropped (producer not connected after ${sendTimeoutMs}ms); topic=${topic} key=${key}`,
          );
          return;
        }
      }
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
    const c = kafka.consumer({ groupId: "order-service" });
    consumer = c;
    // Background connect + subscribe + run, same rationale as producer:
    // a Kafka outage at boot must not block app.listen() or readiness probes.
    const consumerSetupPromise = (async () => {
      while (true) {
        try {
          await c.connect();
          await c.subscribe({ topics: ["payments.events", "inventory.events"] });
          await c.run({
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
          return;
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          console.warn(`order-service Kafka consumer setup failed: ${msg}; retrying in ${reconnectIntervalMs}ms`);
          await new Promise((r) => setTimeout(r, reconnectIntervalMs));
        }
      }
    })();
    consumerSetupPromise.catch(() => {});
  } else {
    console.log("KAFKA_CONSUMERS_ENABLED=false; consumer not started");
  }

  return { producer, consumer, send };
}
