import { describe, expect, it, vi } from "vitest";

vi.mock("../src/exec.js", () => ({
  run: vi.fn(async () => ({ stdout: "", stderr: "" })),
}));

import { run } from "../src/exec.js";
import {
  buildHeaderRulePatch,
  buildDefaultOnlyPatch,
  patchVirtualService,
  rolloutStatus,
  getDeploymentReady,
} from "../src/kubectl.js";

const runMock = vi.mocked(run);

describe("kubectl patch payloads", () => {
  it("buildHeaderRulePatch produces the 2-rule shape with header rule first", () => {
    const patch = JSON.parse(buildHeaderRulePatch("payment-service"));
    expect(patch).toEqual({
      spec: {
        http: [
          {
            name: "canary-by-header",
            match: [{ headers: { "x-canary": { exact: "true" } } }],
            route: [{ destination: { host: "payment-service", subset: "canary" } }],
          },
          {
            name: "default",
            route: [{ destination: { host: "payment-service", subset: "stable" } }],
          },
        ],
      },
    });
  });

  it("buildDefaultOnlyPatch produces the 1-rule shape", () => {
    const patch = JSON.parse(buildDefaultOnlyPatch("order-service"));
    expect(patch).toEqual({
      spec: {
        http: [
          {
            name: "default",
            route: [{ destination: { host: "order-service", subset: "stable" } }],
          },
        ],
      },
    });
  });
});

describe("kubectl shell-outs", () => {
  it("patchVirtualService runs kubectl patch with --type merge", async () => {
    runMock.mockResolvedValue({ stdout: "", stderr: "" });
    await patchVirtualService("payment-service", "services", '{"spec":{"http":[]}}');
    expect(runMock).toHaveBeenCalledWith(
      "kubectl",
      ["patch", "virtualservice", "payment-service", "-n", "services", "--type", "merge", "-p", '{"spec":{"http":[]}}'],
      expect.any(Object),
    );
  });

  it("rolloutStatus runs kubectl rollout status with timeout", async () => {
    runMock.mockResolvedValue({ stdout: "deployment \"payment-service-canary\" successfully rolled out\n", stderr: "" });
    await rolloutStatus("payment-service-canary", "services", 120);
    expect(runMock).toHaveBeenCalledWith(
      "kubectl",
      ["rollout", "status", "deployment/payment-service-canary", "-n", "services", "--timeout=120s"],
      expect.any(Object),
    );
  });

  it("getDeploymentReady reports ready=N/M", async () => {
    runMock.mockResolvedValue({
      stdout: JSON.stringify({ status: { readyReplicas: 1, replicas: 1 } }),
      stderr: "",
    });
    const r = await getDeploymentReady("payment-service-canary", "services");
    expect(r).toEqual({ ready: 1, total: 1, exists: true });
  });

  it("getDeploymentReady reports exists:false on NotFound", async () => {
    runMock.mockResolvedValue({ stdout: "", stderr: 'Error from server (NotFound): deployments.apps "x" not found' });
    const r = await getDeploymentReady("nope-canary", "services");
    expect(r).toEqual({ ready: 0, total: 0, exists: false });
  });
});
