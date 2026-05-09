import { spawn, type ChildProcess } from "node:child_process";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import axios from "axios";

const execFileAsync = promisify(execFile);

export interface PodPortForward {
  localPort: number;
  pod: string;
  stop: () => Promise<void>;
}

/**
 * Returns the name of the first pod matching the label selector that is in the
 * Running phase, in the given namespace.
 */
export async function findPodByLabel(namespace: string, selector: string): Promise<string> {
  const { stdout } = await execFileAsync("kubectl", [
    "-n", namespace,
    "get", "pods",
    "-l", selector,
    "--field-selector=status.phase=Running",
    "-o", "jsonpath={.items[0].metadata.name}",
  ]);
  const name = stdout.trim();
  if (!name) throw new Error(`no Running pod matches ${selector} in ${namespace}`);
  return name;
}

/**
 * Starts a `kubectl port-forward pod/<name> <localPort>:<containerPort>` and
 * waits until the local port responds on /health (or any 2xx/3xx/4xx — anything
 * non-network-error means the forward is live). 30s budget.
 */
export async function portForwardPod(
  namespace: string,
  podName: string,
  localPort: number,
  containerPort: number,
): Promise<PodPortForward> {
  const proc: ChildProcess = spawn("kubectl", [
    "-n", namespace,
    "port-forward",
    `pod/${podName}`,
    `${localPort}:${containerPort}`,
  ], { stdio: ["ignore", "pipe", "pipe"] });

  let stderrBuf = "";
  proc.stderr?.on("data", (d) => (stderrBuf += d.toString()));

  // Wait until the port answers
  const deadline = Date.now() + 30_000;
  let lastErr = "";
  while (Date.now() < deadline) {
    try {
      // Any non-network error is fine — we just need TCP to connect.
      await axios.get(`http://localhost:${localPort}/health`, {
        timeout: 1000,
        validateStatus: () => true,
      });
      return {
        localPort,
        pod: podName,
        stop: () => new Promise<void>((res) => {
          if (proc.exitCode != null) { res(); return; }
          proc.once("exit", () => res());
          proc.kill("SIGTERM");
          setTimeout(() => { try { proc.kill("SIGKILL"); } catch {} ; res(); }, 2000);
        }),
      };
    } catch (e) {
      lastErr = e instanceof Error ? e.message : String(e);
      await new Promise((r) => setTimeout(r, 250));
    }
  }
  proc.kill("SIGKILL");
  throw new Error(`port-forward to ${podName}:${containerPort} not ready on :${localPort} within 30s — last err: ${lastErr}; kubectl stderr: ${stderrBuf}`);
}

/** Sends a POSIX signal to PID 1 inside the named pod. Used by K5. */
export async function sendSignalToPod(namespace: string, pod: string, signal: "STOP" | "CONT" | "KILL"): Promise<void> {
  await execFileAsync("kubectl", [
    "-n", namespace,
    "exec", pod,
    "--", "kill", `-${signal}`, "1",
  ]);
}
