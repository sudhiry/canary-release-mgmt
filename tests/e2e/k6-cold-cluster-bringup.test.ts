import { describe, it, expect, afterAll } from "vitest";
import { spawn } from "node:child_process";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";
import { deployCanary, rollback } from "./helpers/canary.js";
import { findPodByLabel } from "./helpers/pod-port-forward.js";

const execFileAsync = promisify(execFile);

// K6 is gated behind RUN_COLD_CLUSTER_TESTS=true because it tears down and
// re-deploys all 5 services (~4 min). Default e2e suite skips it; manual
// cluster verification opts in.
const RUN = process.env.RUN_COLD_CLUSTER_TESTS === "true";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
// k6-*.test.ts is at tests/e2e/k6-*.test.ts → 2 levels up to repo root.
const REPO_ROOT = process.env.E2E_REPO_ROOT
  ? resolve(process.env.E2E_REPO_ROOT)
  : resolve(__dirname, "..", "..");

/**
 * Runs `make <target>` from the repo root, streaming stdout/stderr to the
 * test's stdio (so failure output appears in vitest reporters), with a hard
 * timeout. Resolves with exit code 0 on success and rejects otherwise.
 */
function runMake(target: string, timeoutMs: number): Promise<void> {
  return new Promise((res, rej) => {
    const proc = spawn("make", [target], {
      cwd: REPO_ROOT,
      stdio: "inherit",
    });
    const timer = setTimeout(() => {
      try { proc.kill("SIGKILL"); } catch {}
      rej(new Error(`make ${target} timed out after ${timeoutMs}ms`));
    }, timeoutMs);
    proc.on("error", (err) => {
      clearTimeout(timer);
      rej(err);
    });
    proc.on("exit", (code, signal) => {
      clearTimeout(timer);
      if (code === 0) res();
      else rej(new Error(`make ${target} exited with code=${code} signal=${signal}`));
    });
  });
}

(RUN ? describe : describe.skip)(
  "K6 — cold-cluster bring-up succeeds without make pre-warm",
  () => {
    afterAll(async () => {
      try { await rollback("audit-service"); } catch {}
    });

    it("make undeploy-services + make deploy-services + canary-deploy without pre-warm", async () => {
      // Step A: tear down to a known cold-cluster state
      await runMake("undeploy-services", 180_000);

      // Step B: redeploy services with the default helm --wait (3min). Asserts
      // helm wait does NOT time out — i.e. stable pods become Ready inside the
      // helm budget without any traffic in the cluster.
      await runMake("deploy-services", 360_000);

      // Step C: deploy a canary WITHOUT running pre-warm. Asserts canary Helm
      // install --wait completes — the headline assertion of K6.
      await deployCanary("audit-service", "dev");

      // Step D: confirm canary readiness is 200 within 30s of pod creation.
      // audit-service exposes its actuator on server.port 8083 (see
      // services/audit-service/src/main/resources/application.yml).
      const canaryPod = await findPodByLabel("services", "app=audit-service,version=canary");
      const start = Date.now();
      let ready = false;
      let lastCode = "";
      while (Date.now() - start < 30_000) {
        const probe = await execFileAsync(
          "kubectl",
          [
            "-n", "services",
            "exec", canaryPod, "--",
            "curl", "-s", "-o", "/dev/null", "-w", "%{http_code}",
            "localhost:8083/actuator/health/readiness",
          ],
          { encoding: "utf8" },
        ).catch((err: NodeJS.ErrnoException & { stdout?: string }) => ({
          stdout: err.stdout ?? "",
          stderr: "",
        }));
        lastCode = probe.stdout.trim();
        if (lastCode === "200") {
          ready = true;
          break;
        }
        await new Promise((r) => setTimeout(r, 2_000));
      }
      expect(
        ready,
        `canary readiness never returned 200 within 30s; last code observed: ${lastCode || "<empty>"}`,
      ).toBe(true);
    }, 600_000);
  },
);
