export interface ServiceEntry {
  name: string;
  helmReleaseStable: string;
  helmReleaseCanary: string;
  namespace: string;
  virtualService: string;
  valuesFile: string;
  canaryOverlay: string;
  chartPath: string;
}

const SERVICES = [
  "audit-service",
  "inventory-service",
  "notification-service",
  "order-service",
  "payment-service",
] as const;

function entry(name: string): ServiceEntry {
  return {
    name,
    helmReleaseStable: name,
    helmReleaseCanary: `${name}-canary`,
    namespace: "services",
    virtualService: name,
    valuesFile: `deploy/helm/values/${name}.yaml`,
    canaryOverlay: "deploy/helm/values/canary-overlay.yaml",
    chartPath: "deploy/helm/service-chart",
  };
}

export const REGISTRY: Record<typeof SERVICES[number], ServiceEntry> = Object.fromEntries(
  SERVICES.map((s) => [s, entry(s)]),
) as Record<typeof SERVICES[number], ServiceEntry>;

export function lookup(svc: string): ServiceEntry {
  const e = (REGISTRY as Record<string, ServiceEntry | undefined>)[svc];
  if (!e) {
    const known = Object.keys(REGISTRY).sort().join(", ");
    throw new Error(`unknown service: ${svc} (known: ${known})`);
  }
  return e;
}
