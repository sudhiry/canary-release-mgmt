import { setTimeout as sleep } from "node:timers/promises";
import { lookup } from "../registry.js";
import { listReleases, uninstallCanary } from "../helm.js";
import {
  patchVirtualService,
  buildHeaderRulePatch,
  buildDefaultOnlyPatch,
  getDeploymentReady,
  getVirtualServiceRules,
} from "../kubectl.js";
import { readState, writeState, deleteState } from "../state.js";
import { log } from "../logger.js";

export type ReconcileAction =
  | "no-op"
  | "complete-deploy"
  | "finish-rollback"
  | "rollback-orphan"
  | "rollback-stale-state"
  | "adopt"
  | "clear-stale-state"
  | "remove-orphan-vs-rule"
  | "drift-fix";

export interface ReconcileOpts {
  service: string;
  stateDir: string;
  adopt: boolean;
  reconcileTimeoutMs?: number;
}

export interface ReconcileResult {
  action: ReconcileAction;
  notes: string[];
}

export async function reconcile(opts: ReconcileOpts): Promise<ReconcileResult> {
  const entry = lookup(opts.service);
  const state = readState(opts.stateDir, entry.name);
  const releases = await listReleases(entry.namespace);
  const canaryReleasePresent = releases.some((r) => r.name === entry.helmReleaseCanary);
  const dep = await getDeploymentReady(entry.helmReleaseCanary, entry.namespace);
  const vs = await getVirtualServiceRules(entry.virtualService, entry.namespace);
  const notes: string[] = [];
  let action: ReconcileAction = "no-op";

  // No state file
  if (!state) {
    if (!canaryReleasePresent && !vs.hasHeaderRule) {
      action = "no-op";
    } else if (!canaryReleasePresent && vs.hasHeaderRule) {
      // Orphan VS rule: remove it.
      await patchVirtualService(entry.virtualService, entry.namespace, buildDefaultOnlyPatch(entry.virtualService));
      action = "remove-orphan-vs-rule";
    } else if (canaryReleasePresent && opts.adopt) {
      // Adopt the orphan release.
      writeState(opts.stateDir, {
        service: entry.name,
        phase: "active",
        tag: "adopted",
        deployedAt: new Date().toISOString(),
      });
      if (!vs.hasHeaderRule) {
        await patchVirtualService(entry.virtualService, entry.namespace, buildHeaderRulePatch(entry.virtualService));
      }
      action = "adopt";
    } else {
      // canaryReleasePresent and !adopt → roll back.
      if (vs.hasHeaderRule) {
        await patchVirtualService(entry.virtualService, entry.namespace, buildDefaultOnlyPatch(entry.virtualService));
      }
      await uninstallCanary(entry.helmReleaseCanary, entry.namespace);
      action = "rollback-orphan";
    }
    log("info", `reconcile: ${action}`, { service: entry.name });
    return { action, notes };
  }

  // State file present.
  switch (state.phase) {
    case "deploying":
      if (!canaryReleasePresent) {
        deleteState(opts.stateDir, entry.name);
        action = "clear-stale-state";
      } else {
        // Poll for readiness up to reconcileTimeoutMs (default 30 s).
        const timeoutMs = opts.reconcileTimeoutMs ?? 30000;
        const pollIntervalMs = 2000;
        const deadline = Date.now() + timeoutMs;
        let currentDep = dep;
        while (!(currentDep.exists && currentDep.ready === currentDep.total && currentDep.total > 0) && Date.now() < deadline) {
          await sleep(pollIntervalMs);
          currentDep = await getDeploymentReady(entry.helmReleaseCanary, entry.namespace);
        }
        if (currentDep.exists && currentDep.ready === currentDep.total && currentDep.total > 0) {
          // Now Ready — complete deploy.
          await patchVirtualService(entry.virtualService, entry.namespace, buildHeaderRulePatch(entry.virtualService));
          writeState(opts.stateDir, { ...state, phase: "active" });
          action = "complete-deploy";
        } else {
          // Still not Ready after timeout — roll back.
          if (vs.hasHeaderRule) {
            await patchVirtualService(entry.virtualService, entry.namespace, buildDefaultOnlyPatch(entry.virtualService));
          }
          await uninstallCanary(entry.helmReleaseCanary, entry.namespace);
          deleteState(opts.stateDir, entry.name);
          action = "rollback-stale-state";
        }
      }
      break;

    case "deployment-ready":
      if (!canaryReleasePresent) {
        if (vs.hasHeaderRule) {
          await patchVirtualService(entry.virtualService, entry.namespace, buildDefaultOnlyPatch(entry.virtualService));
        }
        deleteState(opts.stateDir, entry.name);
        action = "rollback-stale-state";
      } else if (!vs.hasHeaderRule) {
        await patchVirtualService(entry.virtualService, entry.namespace, buildHeaderRulePatch(entry.virtualService));
        writeState(opts.stateDir, { ...state, phase: "active" });
        action = "complete-deploy";
      } else {
        writeState(opts.stateDir, { ...state, phase: "active" });
        action = "drift-fix";
      }
      break;

    case "active":
      if (!canaryReleasePresent) {
        if (vs.hasHeaderRule) {
          await patchVirtualService(entry.virtualService, entry.namespace, buildDefaultOnlyPatch(entry.virtualService));
        }
        deleteState(opts.stateDir, entry.name);
        action = "rollback-stale-state";
      } else if (!vs.hasHeaderRule) {
        await patchVirtualService(entry.virtualService, entry.namespace, buildHeaderRulePatch(entry.virtualService));
        action = "drift-fix";
      } else {
        action = "no-op";
      }
      break;

    case "rolling-back":
      if (vs.hasHeaderRule) {
        await patchVirtualService(entry.virtualService, entry.namespace, buildDefaultOnlyPatch(entry.virtualService));
      }
      if (canaryReleasePresent) {
        await uninstallCanary(entry.helmReleaseCanary, entry.namespace);
      }
      deleteState(opts.stateDir, entry.name);
      action = "finish-rollback";
      break;
  }

  log("info", `reconcile: ${action}`, { service: entry.name, phase: state.phase });
  return { action, notes };
}
