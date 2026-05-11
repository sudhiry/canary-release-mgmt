import { Counter, Histogram, Registry, register as defaultRegister } from "prom-client";
import { currentLane } from "./canary-lane-tag.js";

export type Substrate = "http" | "kafka" | "restate";
export type Outcome = "success" | "client_error" | "server_error";

const COUNTER_NAME = "canary_request_total";
const HISTOGRAM_NAME = "canary_request_duration_seconds";
const SHADOW_NAME = "canary_shadow_mismatch_total";

export class CanaryMetrics {
  private readonly counter: Counter<string>;
  private readonly histogram: Histogram<string>;
  private readonly shadow: Counter<string>;

  constructor(
    private readonly serviceName: string,
    private readonly registry: Registry = defaultRegister,
  ) {
    this.counter = new Counter({
      name: COUNTER_NAME,
      help: "Total canary-tagged requests per substrate/service/lane.",
      labelNames: ["substrate", "service", "lane", "outcome", "target"],
      registers: [registry],
    });
    this.histogram = new Histogram({
      name: HISTOGRAM_NAME,
      help: "Canary-tagged request duration in seconds.",
      labelNames: ["substrate", "service", "lane", "target"],
      registers: [registry],
    });
    this.shadow = new Counter({
      name: SHADOW_NAME,
      help: "Canary shadow-read field mismatches.",
      labelNames: ["service", "field"],
      registers: [registry],
    });
  }

  recordHttp(target: string, outcome: Outcome, durationSeconds: number): void {
    this.record("http", target, outcome, durationSeconds);
  }

  recordKafka(target: string, outcome: Outcome, durationSeconds: number): void {
    this.record("kafka", target, outcome, durationSeconds);
  }

  recordRestate(target: string, outcome: Outcome, durationSeconds: number): void {
    this.record("restate", target, outcome, durationSeconds);
  }

  recordShadowMismatch(field: string): void {
    this.shadow.inc({ service: this.serviceName, field });
  }

  /** For service wiring: hand the registry to the metrics endpoint. */
  getRegistry(): Registry {
    return this.registry;
  }

  private record(substrate: Substrate, target: string, outcome: Outcome, durationSeconds: number): void {
    const lane = currentLane();
    this.counter.inc({
      substrate,
      service: this.serviceName,
      lane,
      outcome,
      target,
    });
    this.histogram.observe(
      { substrate, service: this.serviceName, lane, target },
      durationSeconds,
    );
  }
}
