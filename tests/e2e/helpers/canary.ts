import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const execFileAsync = promisify(execFile);

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
// canary.ts is at tests/e2e/helpers/canary.ts → 3 levels up to repo root.
const REPO_ROOT = process.env.E2E_REPO_ROOT
  ? resolve(process.env.E2E_REPO_ROOT)
  : resolve(__dirname, "..", "..", "..");
const CANARY_CTL = resolve(REPO_ROOT, "tools/canary-ctl/bin/canary-ctl");

interface CanaryCtlOpts {
  stateDir?: string;
  json?: boolean;
}

async function run(args: string[], opts: CanaryCtlOpts = {}): Promise<{ stdout: string; stderr: string }> {
  const fullArgs = ["--repo-root", REPO_ROOT];
  if (opts.stateDir) fullArgs.push("--state-dir", opts.stateDir);
  fullArgs.push(...args);
  try {
    const { stdout, stderr } = await execFileAsync("node", [CANARY_CTL, ...fullArgs], {
      maxBuffer: 16 * 1024 * 1024,
      timeout: 5 * 60_000,
    });
    return { stdout: stdout.toString(), stderr: stderr.toString() };
  } catch (err) {
    const e = err as NodeJS.ErrnoException & { stdout?: Buffer | string; stderr?: Buffer | string; code?: number | string };
    const stderr = e.stderr?.toString() ?? "";
    throw new Error(`canary-ctl ${fullArgs.join(" ")} failed (exit ${e.code}): ${stderr || e.message}`);
  }
}

export interface CanaryStatusResult {
  service: string;
  statePhase: "deploying" | "deployment-ready" | "active" | "rolling-back" | null;
  stateTag: string | null;
  helmCanaryPresent: boolean;
  helmCanaryStatus: string | null;
  deploymentReady: number;
  deploymentTotal: number;
  deploymentExists: boolean;
  vsHasHeaderRule: boolean;
  vsRuleNames: string[];
  drift: string[];
}

export async function deployCanary(svc: string, tag: string, opts: CanaryCtlOpts = {}): Promise<void> {
  await run(["deploy-canary", svc, tag], opts);
}

export async function rollback(svc: string, opts: CanaryCtlOpts = {}): Promise<void> {
  await run(["rollback", svc], opts);
}

export async function status(svc: string, opts: CanaryCtlOpts = {}): Promise<CanaryStatusResult> {
  const { stdout } = await run(["status", svc, "--json"], opts);
  return JSON.parse(stdout) as CanaryStatusResult;
}

export async function reconcile(svc: string, opts: CanaryCtlOpts & { adopt?: boolean } = {}): Promise<void> {
  const args = ["reconcile", svc];
  if (opts.adopt) args.push("--adopt");
  await run(args, opts);
}
