import { createRequire } from "node:module";
import { isCanary } from "../x-canary-context.js";

// createRequire is needed in ESM projects to use require() for lazy SDK loading.
const require = createRequire(import.meta.url);

export interface TracingConfig {
  serviceName: string;
  otlpEndpoint: string;
}

const DEFAULT_OTLP = "http://jaeger-collector.istio-system:4317";

/** Pure-function config builder, exported for tests. */
export function buildTracingConfig(
  serviceName: string | undefined,
  env: Record<string, string | undefined>,
): TracingConfig {
  return {
    serviceName: serviceName ?? env.SERVICE_NAME ?? "unknown",
    otlpEndpoint: env.OTLP_TRACING_ENDPOINT ?? DEFAULT_OTLP,
  };
}

/**
 * Initializes the OpenTelemetry NodeSDK. MUST be called before any module
 * to be auto-instrumented is imported. Idempotent — subsequent calls are no-ops.
 *
 * Usage in a service entry point:
 *   import { initTracing } from "@canary/lib-node";
 *   initTracing("order");   // FIRST line; before any other import that uses HTTP/Kafka
 */
let started = false;
export function initTracing(serviceName?: string): void {
  if (started) return;
  started = true;
  // Lazy-load the SDK only when initTracing is actually called — keeps unit tests
  // (which import this module but never call initTracing) from pulling in the
  // heavy SDK dependency graph.
  const { NodeSDK } = require("@opentelemetry/sdk-node");
  const { getNodeAutoInstrumentations } = require("@opentelemetry/auto-instrumentations-node");
  const { OTLPTraceExporter } = require("@opentelemetry/exporter-trace-otlp-grpc");
  const cfg = buildTracingConfig(serviceName, process.env);

  const sdk = new NodeSDK({
    serviceName: cfg.serviceName,
    traceExporter: new OTLPTraceExporter({ url: cfg.otlpEndpoint }),
    instrumentations: [getNodeAutoInstrumentations()],
    spanProcessors: [new CanaryLaneSpanProcessor(cfg.serviceName)],
  });
  sdk.start();
  process.on("SIGTERM", () => { void sdk.shutdown(); });
}

// SpanProcessor interface shape (compatible with both @opentelemetry/api and sdk-trace-node).
interface Span {
  setAttribute(key: string, value: string): void;
}
interface SpanProcessor {
  onStart(span: Span): void;
  onEnd(): void;
  forceFlush(): Promise<void>;
  shutdown(): Promise<void>;
}

class CanaryLaneSpanProcessor implements SpanProcessor {
  constructor(private readonly serviceName: string) {}
  onStart(span: Span): void {
    span.setAttribute("canary.lane", isCanary() ? "canary" : "stable");
    span.setAttribute("canary.service", this.serviceName);
  }
  onEnd(): void {}
  async forceFlush(): Promise<void> {}
  async shutdown(): Promise<void> {}
}
