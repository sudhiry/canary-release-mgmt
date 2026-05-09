import { run } from "./exec.js";

export function buildHeaderRulePatch(svc: string): string {
  return JSON.stringify({
    spec: {
      http: [
        {
          name: "canary-by-header",
          match: [{ headers: { "x-canary": { exact: "true" } } }],
          route: [{ destination: { host: svc, subset: "canary" } }],
        },
        {
          name: "default",
          route: [{ destination: { host: svc, subset: "stable" } }],
        },
      ],
    },
  });
}

export function buildDefaultOnlyPatch(svc: string): string {
  return JSON.stringify({
    spec: {
      http: [
        {
          name: "default",
          route: [{ destination: { host: svc, subset: "stable" } }],
        },
      ],
    },
  });
}

export async function patchVirtualService(name: string, namespace: string, patch: string): Promise<void> {
  await run("kubectl", ["patch", "virtualservice", name, "-n", namespace, "--type", "merge", "-p", patch], {});
}

export async function rolloutStatus(deploy: string, namespace: string, timeoutSeconds: number): Promise<void> {
  await run(
    "kubectl",
    ["rollout", "status", `deployment/${deploy}`, "-n", namespace, `--timeout=${timeoutSeconds}s`],
    { timeoutMs: (timeoutSeconds + 5) * 1000 },
  );
}

export interface DeploymentReady {
  ready: number;
  total: number;
  exists: boolean;
}

export async function getDeploymentReady(name: string, namespace: string): Promise<DeploymentReady> {
  const r = await run(
    "kubectl",
    ["get", "deployment", name, "-n", namespace, "-o", "json"],
    { ignoreError: true },
  );
  if (r.stderr.includes("NotFound") || /not found/i.test(r.stderr)) {
    return { ready: 0, total: 0, exists: false };
  }
  const obj = JSON.parse(r.stdout) as { status?: { readyReplicas?: number; replicas?: number } };
  return {
    ready: obj.status?.readyReplicas ?? 0,
    total: obj.status?.replicas ?? 0,
    exists: true,
  };
}

export interface VirtualServiceRules {
  hasHeaderRule: boolean;
  ruleNames: string[];
}

export async function getVirtualServiceRules(name: string, namespace: string): Promise<VirtualServiceRules> {
  const r = await run(
    "kubectl",
    ["get", "virtualservice", name, "-n", namespace, "-o", "json"],
    { ignoreError: true },
  );
  if (r.stderr.includes("NotFound")) {
    return { hasHeaderRule: false, ruleNames: [] };
  }
  const obj = JSON.parse(r.stdout) as { spec?: { http?: Array<{ name?: string }> } };
  const names = (obj.spec?.http ?? []).map((h) => h.name ?? "");
  return {
    hasHeaderRule: names.includes("canary-by-header"),
    ruleNames: names,
  };
}
