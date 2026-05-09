import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { log } from "./logger.js";

const execFileAsync = promisify(execFile);

export interface RunResult {
  stdout: string;
  stderr: string;
}

export interface RunOptions {
  cwd?: string;
  timeoutMs?: number;
  /** If true, do not throw on non-zero exit; return result with stderr filled. */
  ignoreError?: boolean;
}

export async function run(cmd: string, args: string[], opts: RunOptions = {}): Promise<RunResult> {
  log("debug", "exec", { cmd, args, cwd: opts.cwd });
  try {
    const { stdout, stderr } = await execFileAsync(cmd, args, {
      cwd: opts.cwd,
      timeout: opts.timeoutMs,
      maxBuffer: 16 * 1024 * 1024,
    });
    return { stdout: stdout.toString(), stderr: stderr.toString() };
  } catch (err) {
    const e = err as NodeJS.ErrnoException & { stdout?: Buffer | string; stderr?: Buffer | string; code?: number | string };
    if (opts.ignoreError) {
      return {
        stdout: e.stdout?.toString() ?? "",
        stderr: e.stderr?.toString() ?? e.message,
      };
    }
    const stderr = e.stderr?.toString() ?? "";
    throw new Error(`${cmd} ${args.join(" ")} failed (exit ${e.code}): ${stderr || e.message}`);
  }
}
