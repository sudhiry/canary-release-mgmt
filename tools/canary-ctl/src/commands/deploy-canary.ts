import { resolve } from "node:path";
import { lookup } from "../registry.js";
import { upgradeInstallCanary, uninstallCanary } from "../helm.js";
import {
  patchVirtualService,
  buildHeaderRulePatch,
  buildDefaultOnlyPatch,
} from "../kubectl.js";
import { writeState, deleteState } from "../state.js";
import { log } from "../logger.js";

export interface DeployCanaryOpts {
  service: string;
  tag: string;
  stateDir: string;
  repoRoot: string;
  timeoutSeconds?: number;
}

export async function deployCanary(opts: DeployCanaryOpts): Promise<void> {
  const entry = lookup(opts.service);
  const timeout = opts.timeoutSeconds ?? 120;

  // Phase: deploying
  writeState(opts.stateDir, {
    service: entry.name,
    phase: "deploying",
    tag: opts.tag,
    deployedAt: new Date().toISOString(),
  });
  log("info", "phase=deploying", { service: entry.name, tag: opts.tag });

  try {
    await upgradeInstallCanary({
      releaseName: entry.helmReleaseCanary,
      namespace: entry.namespace,
      chartPath: resolve(opts.repoRoot, entry.chartPath),
      valuesFile: resolve(opts.repoRoot, entry.valuesFile),
      canaryOverlay: resolve(opts.repoRoot, entry.canaryOverlay),
      tag: opts.tag,
      timeoutSeconds: timeout,
    });
  } catch (err) {
    await autoRollback(opts, entry.name, entry.helmReleaseCanary, entry.virtualService, entry.namespace);
    throw err;
  }

  // Phase: deployment-ready
  writeState(opts.stateDir, {
    service: entry.name,
    phase: "deployment-ready",
    tag: opts.tag,
    deployedAt: new Date().toISOString(),
  });
  log("info", "phase=deployment-ready", { service: entry.name });

  // Apply header rule.
  try {
    await patchVirtualService(entry.virtualService, entry.namespace, buildHeaderRulePatch(entry.virtualService));
  } catch (err) {
    await autoRollback(opts, entry.name, entry.helmReleaseCanary, entry.virtualService, entry.namespace);
    throw err;
  }

  // Phase: active
  writeState(opts.stateDir, {
    service: entry.name,
    phase: "active",
    tag: opts.tag,
    deployedAt: new Date().toISOString(),
  });
  log("info", "phase=active", { service: entry.name, tag: opts.tag });
}

async function autoRollback(
  opts: DeployCanaryOpts,
  service: string,
  releaseName: string,
  vsName: string,
  namespace: string,
): Promise<void> {
  log("warn", "auto-rollback", { service });
  writeState(opts.stateDir, {
    service,
    phase: "rolling-back",
    tag: opts.tag,
    deployedAt: new Date().toISOString(),
  });
  // Header rule first (idempotent — no-op if it was never applied).
  try {
    await patchVirtualService(vsName, namespace, buildDefaultOnlyPatch(vsName));
  } catch (e) {
    log("warn", "auto-rollback: VS patch failed (continuing)", { error: (e as Error).message });
  }
  // Helm uninstall (ignoreError=true inside helm.uninstallCanary).
  await uninstallCanary(releaseName, namespace);
  deleteState(opts.stateDir, service);
}
