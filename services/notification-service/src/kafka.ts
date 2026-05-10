import { Kafka, type Producer, type Consumer, type EachMessagePayload } from "kafkajs";
import {
  stampXCanaryOnProducerRecord,
  resolveConsumerGroupId,
  shouldProcess,
  runWithCanaryFromHeaders,
  createKafkaHealthState,
  type KafkaHealthState,
  type KafkaConsumeHeaders,
  XCanaryPresenceWatcher,
} from "@canary/lib-node";
import { consumedEventStore } from "./store.js";

export interface KafkaSetupOptions {
  brokers: string[];
  consumersEnabled: boolean;
  producerEnabled: boolean;
  /** Service version: "stable" | "canary". Defaults to process.env.VERSION || "stable". */
  ownVersion?: string;
  /** K8s namespace for the presence watcher. Defaults to process.env.POD_NAMESPACE || "services". */
  namespace?: string;
  /** Disable the presence watcher entirely (e.g., for tests). */
  presenceWatcherEnabled?: boolean;
  /** Override heartbeat-staleness threshold in ms. Defaults to 15000. */
  heartbeatStaleMs?: number;
  sendTimeoutMs?: number;
  reconnectIntervalMs?: number;
  /**
   * Test-only: override the isCanaryReady supplier so tests can exercise the
   * stable+canaryReady filter cell without a real Kubernetes cluster.
   * @internal
   */
  _testIsCanaryReady?: () => boolean;
}

export interface KafkaHandle {
  producer: Producer | null;
  consumer: Consumer | null;
  send: (topic: string, key: string, value: string) => Promise<void>;
  health: KafkaHealthState;
  presenceWatcher: XCanaryPresenceWatcher | null;
}

export async function setupKafka(opts: KafkaSetupOptions): Promise<KafkaHandle> {
  const ownVersion = opts.ownVersion ?? process.env.VERSION ?? "stable";
  const namespace = opts.namespace ?? process.env.POD_NAMESPACE ?? "services";
  const watcherEnabled = opts.presenceWatcherEnabled ?? true;
  const health = createKafkaHealthState(opts.heartbeatStaleMs ?? 15000);

  const kafka = new Kafka({ clientId: "notification-service", brokers: opts.brokers });
  const sendTimeoutMs = opts.sendTimeoutMs ?? 5000;
  const reconnectIntervalMs = opts.reconnectIntervalMs ?? 10000;

  let presenceWatcher: XCanaryPresenceWatcher | null = null;
  if (watcherEnabled && ownVersion === "stable") {
    presenceWatcher = new XCanaryPresenceWatcher(namespace, "notification-service");
    await presenceWatcher.start().catch((err) => {
      console.warn(`notification-service presence watcher start failed: ${err}; canary will be treated as not-ready`);
    });
  }

  // isCanaryReady supplier: prefer test override, then watcher, then false.
  const isCanaryReady: () => boolean = opts._testIsCanaryReady
    ? opts._testIsCanaryReady
    : () => (presenceWatcher ? presenceWatcher.isCanaryReady() : false);

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
          console.log("notification-service Kafka producer connected");
          return;
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          console.warn(`notification-service Kafka producer connect failed: ${msg}; retrying in ${reconnectIntervalMs}ms`);
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
            `notification-service kafka send dropped (producer not connected after ${sendTimeoutMs}ms); topic=${topic} key=${key}`,
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
    const groupId = resolveConsumerGroupId("notification-service");
    const c = kafka.consumer({ groupId });
    consumer = c;
    // Heartbeat-based readiness: drive the KafkaHealthState machine off kafkajs's
    // own consumer events. GROUP_JOIN/REBALANCING gate the "assigned" flag and
    // HEARTBEAT advances liveness; DISCONNECT resets us to unassigned.
    c.on(c.events.GROUP_JOIN, () => health.markAssigned());
    c.on(c.events.REBALANCING, () => health.markRevoked());
    c.on(c.events.HEARTBEAT, () => health.recordHeartbeat());
    c.on(c.events.DISCONNECT, () => health.markRevoked());
    // Background connect + subscribe + run, same rationale as producer:
    // a Kafka outage at boot must not block app.listen() or readiness probes.
    const consumerSetupPromise = (async () => {
      while (true) {
        try {
          await c.connect();
          await c.subscribe({ topics: ["orders.events", "payments.events"] });
          await c.run({
            eachMessage: async ({ topic, message }: EachMessagePayload) => {
              // KafkaJS IHeaders is a superset of KafkaConsumeHeaders; lib only reads
              // Buffer values (calls .toString("utf8")), so the cast is safe.
              const headers = message.headers as KafkaConsumeHeaders;
              if (!shouldProcess(headers, ownVersion, isCanaryReady)) {
                return;
              }
              await runWithCanaryFromHeaders(headers, async () => {
                const stringHeaders: Record<string, string> = {};
                for (const [k, v] of Object.entries(message.headers ?? {})) {
                  if (v) stringHeaders[k] = Buffer.isBuffer(v) ? v.toString("utf8") : String(v);
                }
                consumedEventStore.record({
                  topic,
                  key: message.key?.toString("utf8") ?? null,
                  value: message.value?.toString("utf8") ?? "",
                  headers: stringHeaders,
                });
              });
            },
          });
          console.log(`notification-service Kafka consumer subscribed (groupId=${groupId})`);
          return;
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          console.warn(`notification-service Kafka consumer setup failed: ${msg}; retrying in ${reconnectIntervalMs}ms`);
          await new Promise((r) => setTimeout(r, reconnectIntervalMs));
        }
      }
    })();
    consumerSetupPromise.catch(() => {});
  } else {
    console.log("KAFKA_CONSUMERS_ENABLED=false; consumer not started");
  }

  return { producer, consumer, send, health, presenceWatcher };
}
