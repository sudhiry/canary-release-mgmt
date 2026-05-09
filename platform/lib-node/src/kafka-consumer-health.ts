export interface KafkaHealthReport {
  ok: boolean;
  reason?: string;
  ageMs?: number;
}

export interface KafkaHealthState {
  recordPoll(): void;
  isHealthy(): boolean;
  report(): KafkaHealthReport;
}

export function createKafkaHealthState(timeoutMs: number = 30_000): KafkaHealthState {
  let lastPollMs = 0;
  return {
    recordPoll() {
      lastPollMs = Date.now();
    },
    isHealthy() {
      if (lastPollMs === 0) return false;
      return Date.now() - lastPollMs <= timeoutMs;
    },
    report() {
      if (lastPollMs === 0) {
        return { ok: false, reason: "no poll yet" };
      }
      const ageMs = Date.now() - lastPollMs;
      if (ageMs > timeoutMs) {
        return { ok: false, reason: `stale ${Math.floor(ageMs / 1000)}s`, ageMs };
      }
      return { ok: true, ageMs };
    },
  };
}
