import axios from "axios";
import { findPodByLabel, portForwardPod, type PodPortForward } from "./pod-port-forward.js";

export interface ConsumedEventRow {
  topic: string;
  key: string | null;
  value: string;
  headers: Record<string, string>;
}

/** Container port each service listens on (matches Helm per-service values). */
export const SERVICE_CONTAINER_PORT: Record<string, number> = {
  "order-service": 3001,
  "notification-service": 3002,
  "payment-service": 8081,
  "inventory-service": 8082,
  "audit-service": 8083,
};

/** Local port allocator base — start at 18000 to avoid collisions with traffic.ts (8080) and dashboards. */
let nextLocalPort = 18000;

export async function openSubsetForward(
  service: string,
  subset: "stable" | "canary",
): Promise<PodPortForward> {
  const pod = await findPodByLabel("services", `app=${service},version=${subset}`);
  const containerPort = SERVICE_CONTAINER_PORT[service];
  if (!containerPort) throw new Error(`no container port mapped for service ${service}`);
  const local = nextLocalPort++;
  return portForwardPod("services", pod, local, containerPort);
}

export async function getConsumedEvents(forward: PodPortForward): Promise<ConsumedEventRow[]> {
  const r = await axios.get(`http://localhost:${forward.localPort}/internal/consumed-events`, {
    validateStatus: () => true,
    timeout: 5000,
  });
  if (r.status !== 200) {
    throw new Error(`consumed-events fetch failed (pod=${forward.pod}): status=${r.status} body=${JSON.stringify(r.data)}`);
  }
  if (!Array.isArray(r.data)) {
    throw new Error(`consumed-events response not an array (pod=${forward.pod}): ${JSON.stringify(r.data)}`);
  }
  return r.data as ConsumedEventRow[];
}

export async function waitForConsumed(
  forward: PodPortForward,
  predicate: (rows: ConsumedEventRow[]) => boolean,
  timeoutMs = 15000,
  pollMs = 250,
): Promise<ConsumedEventRow[]> {
  const deadline = Date.now() + timeoutMs;
  let last: ConsumedEventRow[] = [];
  while (Date.now() < deadline) {
    try {
      last = await getConsumedEvents(forward);
      if (predicate(last)) return last;
    } catch (e) {
      // Keep trying — port-forward may have hiccupped briefly.
    }
    await new Promise((r) => setTimeout(r, pollMs));
  }
  throw new Error(`waitForConsumed(pod=${forward.pod}) timed out after ${timeoutMs}ms; last=${JSON.stringify(last).slice(0, 500)}`);
}
