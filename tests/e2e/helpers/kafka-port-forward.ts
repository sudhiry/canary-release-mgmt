import { spawn, type ChildProcess } from "node:child_process";
import { createConnection } from "node:net";

export interface KafkaPortForward {
  stop: () => Promise<void>;
  localPort: number;
}

export async function startKafkaPortForward(localPort = 9092): Promise<KafkaPortForward> {
  const child: ChildProcess = spawn(
    "kubectl",
    ["port-forward", "-n", "kafka", "svc/my-cluster-kafka-bootstrap", `${localPort}:9092`],
    { stdio: ["ignore", "pipe", "pipe"], detached: false },
  );

  let stderrBuf = "";
  child.stderr?.on("data", (d: Buffer) => { stderrBuf += d.toString(); });

  const deadline = Date.now() + 30_000;
  let lastErr = "";
  while (Date.now() < deadline) {
    try {
      await new Promise<void>((resolve, reject) => {
        const sock = createConnection({ port: localPort, host: "127.0.0.1" });
        sock.once("connect", () => { sock.end(); resolve(); });
        sock.once("error", reject);
      });
      return {
        localPort,
        stop: async () => {
          if (!child.killed) child.kill("SIGTERM");
          await new Promise<void>((res) => child.once("exit", () => res()));
        },
      };
    } catch (e) {
      lastErr = (e as Error).message;
      await new Promise<void>((r) => setTimeout(r, 250));
    }
  }
  child.kill("SIGTERM");
  throw new Error(`kafka port-forward failed to become ready on :${localPort} within 30s — last err: ${lastErr}; kubectl stderr: ${stderrBuf}`);
}
