import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

vi.mock("../src/helm.js", () => ({
  upgradeInstallCanary: vi.fn(async () => {}),
  uninstallCanary: vi.fn(async () => {}),
  listReleases: vi.fn(async () => []),
}));
vi.mock("../src/kubectl.js", () => ({
  patchVirtualService: vi.fn(async () => {}),
  rolloutStatus: vi.fn(async () => {}),
  buildHeaderRulePatch: (svc: string) => `HEADER:${svc}`,
  buildDefaultOnlyPatch: (svc: string) => `DEFAULT:${svc}`,
  getDeploymentReady: vi.fn(),
  getVirtualServiceRules: vi.fn(),
}));

import { upgradeInstallCanary, uninstallCanary } from "../src/helm.js";
import { patchVirtualService } from "../src/kubectl.js";
import { readState, deleteState, type CanaryState } from "../src/state.js";
import { deployCanary } from "../src/commands/deploy-canary.js";

const upgradeMock = vi.mocked(upgradeInstallCanary);
const uninstallMock = vi.mocked(uninstallCanary);
const patchMock = vi.mocked(patchVirtualService);

describe("deploy-canary", () => {
  let dir: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "deploy-canary-"));
    upgradeMock.mockReset().mockResolvedValue(undefined);
    uninstallMock.mockReset().mockResolvedValue(undefined);
    patchMock.mockReset().mockResolvedValue(undefined);
  });

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  it("happy path: upgrade, then patch VS, then state phase=active", async () => {
    await deployCanary({ service: "payment-service", tag: "v2", stateDir: dir, repoRoot: "/repo" });

    expect(upgradeMock).toHaveBeenCalledOnce();
    expect(patchMock).toHaveBeenCalledWith("payment-service", "services", "HEADER:payment-service");
    const state = readState(dir, "payment-service") as CanaryState;
    expect(state.phase).toBe("active");
    expect(state.tag).toBe("v2");
    expect(state.deployedAt).toMatch(/^\d{4}-\d{2}-\d{2}T/);
  });

  it("helm failure auto-rolls back and rethrows", async () => {
    upgradeMock.mockRejectedValueOnce(new Error("rollout deadline exceeded"));
    await expect(
      deployCanary({ service: "payment-service", tag: "nope", stateDir: dir, repoRoot: "/repo" }),
    ).rejects.toThrow(/rollout deadline/);

    // Auto-rollback fired:
    expect(patchMock).toHaveBeenCalledWith("payment-service", "services", "DEFAULT:payment-service");
    expect(uninstallMock).toHaveBeenCalledWith("payment-service-canary", "services");
    expect(readState(dir, "payment-service")).toBeNull();
  });

  it("patch failure after successful helm install auto-rolls back", async () => {
    patchMock.mockRejectedValueOnce(new Error("kubectl patch error"));
    await expect(
      deployCanary({ service: "order-service", tag: "v2", stateDir: dir, repoRoot: "/repo" }),
    ).rejects.toThrow(/kubectl patch error/);

    // Auto-rollback uninstalls the release.
    expect(uninstallMock).toHaveBeenCalledWith("order-service-canary", "services");
    expect(readState(dir, "order-service")).toBeNull();
  });

  it("unknown service throws before any side effects", async () => {
    await expect(
      deployCanary({ service: "nope", tag: "v2", stateDir: dir, repoRoot: "/repo" }),
    ).rejects.toThrow(/unknown service: nope/i);
    expect(upgradeMock).not.toHaveBeenCalled();
  });
});
