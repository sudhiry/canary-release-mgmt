import { describe, expect, it } from "vitest";
import { lookup, REGISTRY, type ServiceEntry } from "../src/registry.js";

describe("registry", () => {
  it("contains all 5 Phase 1 services", () => {
    expect(Object.keys(REGISTRY).sort()).toEqual([
      "audit-service",
      "inventory-service",
      "notification-service",
      "order-service",
      "payment-service",
    ]);
  });

  it("each entry has the expected shape", () => {
    for (const [name, entry] of Object.entries(REGISTRY)) {
      expect(entry.name).toBe(name);
      expect(entry.helmReleaseStable).toBe(name);
      expect(entry.helmReleaseCanary).toBe(`${name}-canary`);
      expect(entry.namespace).toBe("services");
      expect(entry.virtualService).toBe(name);
      expect(entry.valuesFile).toBe(`deploy/helm/values/${name}.yaml`);
      expect(entry.canaryOverlay).toBe("deploy/helm/values/canary-overlay.yaml");
      expect(entry.chartPath).toBe("deploy/helm/service-chart");
    }
  });

  it("lookup returns the entry for a known service", () => {
    const e: ServiceEntry = lookup("payment-service");
    expect(e.helmReleaseCanary).toBe("payment-service-canary");
  });

  it("lookup throws for an unknown service", () => {
    expect(() => lookup("not-a-real-service")).toThrow(
      /unknown service: not-a-real-service/i,
    );
  });

  it("lookup is case-sensitive", () => {
    expect(() => lookup("Payment-Service")).toThrow(/unknown service/i);
    expect(() => lookup("PAYMENT-SERVICE")).toThrow(/unknown service/i);
  });
});
