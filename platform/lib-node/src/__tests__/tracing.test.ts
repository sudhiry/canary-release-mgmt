import { describe, expect, it } from "vitest";
import { buildTracingConfig } from "../observability/tracing.js";

describe("buildTracingConfig", () => {
  it("uses default OTLP endpoint when env not set", () => {
    const cfg = buildTracingConfig("order", {});
    expect(cfg.serviceName).toBe("order");
    expect(cfg.otlpEndpoint).toBe("http://jaeger-collector.istio-system:4317");
  });

  it("honors OTLP_TRACING_ENDPOINT env var", () => {
    const cfg = buildTracingConfig("notification", { OTLP_TRACING_ENDPOINT: "http://otel:4317" });
    expect(cfg.otlpEndpoint).toBe("http://otel:4317");
  });

  it("derives serviceName from arg, falls back to SERVICE_NAME env", () => {
    const cfg = buildTracingConfig(undefined, { SERVICE_NAME: "audit" });
    expect(cfg.serviceName).toBe("audit");
  });
});
