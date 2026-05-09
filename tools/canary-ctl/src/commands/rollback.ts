import { setTimeout as sleep } from "node:timers/promises";
import { lookup } from "../registry.js";
import { uninstallCanary, listReleases } from "../helm.js";
import { patchVirtualService, buildDefaultOnlyPatch } from "../kubectl.js";
import { readState, deleteState, writeState } from "../state.js";
import { log } from "../logger.js";

export interface RollbackOpts {
  service: string;
  stateDir: string;
  graceSeconds: number;
}

export async function rollback(opts: RollbackOpts): Promise<void> {
  const entry = lookup(opts.service);
  const state = readState(opts.stateDir, entry.name);
  const releases = await listReleases(entry.namespace);
  const canaryReleasePresent = releases.some((r) => r.name === entry.helmReleaseCanary);

  if (!state && !canaryReleasePresent) {
    log("info", "rollback: nothing to do", { service: entry.name });
    return;
  }

  // Phase: rolling-back (only if state existed)
  if (state) {
    writeState(opts.stateDir, { ...state, phase: "rolling-back" });
  }

  // 1. Header rule off (idempotent).
  await patchVirtualService(
    entry.virtualService,
    entry.namespace,
    buildDefaultOnlyPatch(entry.virtualService),
  );
  log("info", "rollback: VS reverted to default-only", { service: entry.name });

  // 2. Grace.
  if (opts.graceSeconds > 0) {
    log("info", "rollback: grace sleep", { seconds: opts.graceSeconds });
    await sleep(opts.graceSeconds * 1000);
  }

  // 3. Uninstall canary release (idempotent if absent — helm uninstall ignoreError).
  if (canaryReleasePresent) {
    await uninstallCanary(entry.helmReleaseCanary, entry.namespace);
    log("info", "rollback: canary release uninstalled", { release: entry.helmReleaseCanary });
  }

  // 4. Clear state.
  deleteState(opts.stateDir, entry.name);
  log("info", "rollback: state cleared", { service: entry.name });
}
