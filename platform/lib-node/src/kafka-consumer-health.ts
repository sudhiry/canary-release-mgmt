export interface KafkaHealthReport {
  ok: boolean;
  reason?: string;
  ageMs?: number;
}

export interface KafkaHealthState {
  markAssigned(): void;
  markRevoked(): void;
  recordHeartbeat(): void;
  isHealthy(): boolean;
  report(): KafkaHealthReport;
}

export function createKafkaHealthState(heartbeatStaleMs: number = 15_000): KafkaHealthState {
  let assigned = false;
  let lastHeartbeatMs = 0;
  return {
    markAssigned() {
      assigned = true;
    },
    markRevoked() {
      assigned = false;
    },
    recordHeartbeat() {
      lastHeartbeatMs = Date.now();
    },
    isHealthy() {
      if (!assigned) return false;
      if (lastHeartbeatMs === 0) return false;
      return Date.now() - lastHeartbeatMs <= heartbeatStaleMs;
    },
    report() {
      if (!assigned) return { ok: false, reason: "no partitions assigned" };
      if (lastHeartbeatMs === 0) return { ok: false, reason: "no heartbeat yet" };
      const ageMs = Date.now() - lastHeartbeatMs;
      if (ageMs > heartbeatStaleMs) {
        return { ok: false, reason: `stale ${Math.floor(ageMs / 1000)}s`, ageMs };
      }
      return { ok: true, ageMs };
    },
  };
}
