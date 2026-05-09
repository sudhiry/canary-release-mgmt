import { execFile } from "node:child_process";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

export interface PodLogQueryOpts {
  namespace: string;
  labelSelector: string;       // e.g. "app=order-service,version=canary"
  sinceSeconds?: number;       // default 60
}

export async function getPodLogs(opts: PodLogQueryOpts): Promise<string> {
  const args = [
    "logs",
    "-n", opts.namespace,
    "-l", opts.labelSelector,
    `--since=${opts.sinceSeconds ?? 60}s`,
    "--tail=-1",
    "--prefix=true",
  ];
  try {
    const { stdout } = await execFileAsync("kubectl", args, {
      maxBuffer: 32 * 1024 * 1024,
      timeout: 30_000,
    });
    return stdout.toString();
  } catch (err) {
    const e = err as NodeJS.ErrnoException & { stderr?: Buffer | string; code?: number | string };
    const stderr = e.stderr?.toString() ?? "";
    throw new Error(`kubectl logs failed (exit ${e.code}): ${stderr || e.message}`);
  }
}

export function logsContain(logs: string, pattern: RegExp | string): boolean {
  if (pattern instanceof RegExp) return pattern.test(logs);
  return logs.includes(pattern);
}
