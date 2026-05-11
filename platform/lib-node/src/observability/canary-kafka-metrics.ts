import type { Consumer, ConsumerRunConfig, EachMessagePayload } from "kafkajs";
import type { CanaryMetrics } from "./canary-metrics.js";

/**
 * Returns a wrapped Consumer whose `run({ eachMessage })` callback is timed
 * and recorded into CanaryMetrics. Other Consumer methods pass through.
 */
export function wrapKafkaConsumer(consumer: Consumer, metrics: CanaryMetrics): Consumer {
  const originalRun = consumer.run.bind(consumer);
  consumer.run = async function (config?: ConsumerRunConfig): Promise<void> {
    if (!config?.eachMessage) {
      return originalRun(config);
    }
    const userEachMessage = config.eachMessage;
    const wrapped: ConsumerRunConfig = {
      ...config,
      eachMessage: async (payload: EachMessagePayload) => {
        const startNs = process.hrtime.bigint();
        try {
          await userEachMessage(payload);
          const elapsed = Number(process.hrtime.bigint() - startNs) / 1e9;
          metrics.recordKafka(payload.topic, "success", elapsed);
        } catch (err) {
          const elapsed = Number(process.hrtime.bigint() - startNs) / 1e9;
          metrics.recordKafka(payload.topic, "server_error", elapsed);
          throw err;
        }
      },
    };
    return originalRun(wrapped);
  };
  return consumer;
}
