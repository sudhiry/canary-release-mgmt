import { existsSync, mkdirSync, readFileSync, renameSync, unlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";

export type CanaryPhase = "deploying" | "deployment-ready" | "active" | "rolling-back";

export interface CanaryState {
  service: string;
  phase: CanaryPhase;
  tag: string;
  deployedAt: string;
}

function pathFor(dir: string, svc: string): string {
  return join(dir, `${svc}.json`);
}

export function readState(dir: string, svc: string): CanaryState | null {
  const p = pathFor(dir, svc);
  if (!existsSync(p)) return null;
  const raw = readFileSync(p, "utf8");
  try {
    return JSON.parse(raw) as CanaryState;
  } catch (e) {
    throw new Error(`invalid state file at ${p}: ${(e as Error).message}`);
  }
}

export function writeState(dir: string, state: CanaryState): void {
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  const target = pathFor(dir, state.service);
  const tmp = `${target}.tmp`;
  writeFileSync(tmp, JSON.stringify(state, null, 2), "utf8");
  renameSync(tmp, target);
}

export function deleteState(dir: string, svc: string): void {
  const p = pathFor(dir, svc);
  if (!existsSync(p)) return;
  unlinkSync(p);
}
