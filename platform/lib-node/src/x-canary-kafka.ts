import type { ProducerRecord, IHeaders } from "kafkajs";
import { isCanary } from "./x-canary-context.js";
import { X_CANARY_HEADER, X_CANARY_TRUE } from "./x-canary-constants.js";

/**
 * Returns the same record with x-canary stamped on every message when the
 * caller's XCanaryContext is canary. Pure function — no Kafka client coupling.
 * Wire this in as a wrapper around producer.send():
 *
 *   await producer.send(stampXCanaryOnProducerRecord(record));
 */
export function stampXCanaryOnProducerRecord(record: ProducerRecord): ProducerRecord {
  if (!isCanary()) {
    return record;
  }
  const messages = record.messages.map((message) => {
    const headers: IHeaders = { ...(message.headers ?? {}) };
    if (headers[X_CANARY_HEADER] === undefined) {
      headers[X_CANARY_HEADER] = X_CANARY_TRUE;
    }
    return { ...message, headers };
  });
  return { ...record, messages };
}
