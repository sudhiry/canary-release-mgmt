import { lookup } from "../registry.js";
import { listReleases } from "../helm.js";
import { getDeploymentReady, getVirtualServiceRules } from "../kubectl.js";
import { readState, type CanaryPhase } from "../state.js";

export interface StatusOpts {
  service: string;
  stateDir: string;
}

export interface CanaryStatus {
  service: string;
  statePhase: CanaryPhase | null;
  stateTag: string | null;
  stateDeployedAt: string | null;
  helmCanaryPresent: boolean;
  helmCanaryStatus: string | null;
  deploymentReady: number;
  deploymentTotal: number;
  deploymentExists: boolean;
  vsHasHeaderRule: boolean;
  vsRuleNames: string[];
  drift: string[];
}

export async function computeStatus(opts: StatusOpts): Promise<CanaryStatus> {
  const entry = lookup(opts.service);
  const state = readState(opts.stateDir, entry.name);
  const releases = await listReleases(entry.namespace);
  const canary = releases.find((r) => r.name === entry.helmReleaseCanary);
  const dep = await getDeploymentReady(entry.helmReleaseCanary, entry.namespace);
  const vs = await getVirtualServiceRules(entry.virtualService, entry.namespace);

  const drift: string[] = [];
  if (state && state.phase === "active" && !vs.hasHeaderRule) {
    drift.push("state phase=active but VS has no header rule");
  }
  if (state && state.phase === "active" && !canary) {
    drift.push("state phase=active but canary release missing");
  }
  if (!state && canary) {
    drift.push("state file missing but canary release present");
  }
  if (!state && vs.hasHeaderRule) {
    drift.push("state file missing but VS still has header rule");
  }

  return {
    service: entry.name,
    statePhase: state?.phase ?? null,
    stateTag: state?.tag ?? null,
    stateDeployedAt: state?.deployedAt ?? null,
    helmCanaryPresent: Boolean(canary),
    helmCanaryStatus: canary?.status ?? null,
    deploymentReady: dep.ready,
    deploymentTotal: dep.total,
    deploymentExists: dep.exists,
    vsHasHeaderRule: vs.hasHeaderRule,
    vsRuleNames: vs.ruleNames,
    drift,
  };
}

export function formatStatusText(s: CanaryStatus): string {
  const lines: string[] = [];
  lines.push(`${s.service}:`);
  lines.push(`  state file: ${s.statePhase ?? "absent"}${s.statePhase ? ` (tag ${s.stateTag}, deployed ${s.stateDeployedAt})` : ""}`);
  lines.push(`  helm release ${s.service}-canary: ${s.helmCanaryPresent ? `present, status ${s.helmCanaryStatus}` : "absent"}`);
  if (s.deploymentExists) {
    lines.push(`  deployment ready: ${s.deploymentReady}/${s.deploymentTotal}`);
  }
  lines.push(`  virtualservice header rule: ${s.vsHasHeaderRule ? "present" : "absent"} (rules: ${s.vsRuleNames.join(", ") || "none"})`);
  lines.push(`  drift: ${s.drift.length === 0 ? "none" : ""}`);
  for (const d of s.drift) lines.push(`    - ${d}`);
  return lines.join("\n");
}
