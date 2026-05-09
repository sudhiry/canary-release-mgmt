import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

vi.mock("../src/helm.js", () => ({
  listReleases: vi.fn(),
  uninstallCanary: vi.fn(async () => {}),
}));
vi.mock("../src/kubectl.js", () => ({
  patchVirtualService: vi.fn(async () => {}),
  buildHeaderRulePatch: (svc: string) => `HEADER:${svc}`,
  buildDefaultOnlyPatch: (svc: string) => `DEFAULT:${svc}`,
  getDeploymentReady: vi.fn(),
  getVirtualServiceRules: vi.fn(),
}));

import { listReleases, uninstallCanary } from "../src/helm.js";
import { patchVirtualService, getDeploymentReady, getVirtualServiceRules } from "../src/kubectl.js";
import { writeState, readState } from "../src/state.js";
import { reconcile } from "../src/commands/reconcile.js";

const listMock = vi.mocked(listReleases);
const uninstallMock = vi.mocked(uninstallCanary);
const patchMock = vi.mocked(patchVirtualService);
const depMock = vi.mocked(getDeploymentReady);
const vsMock = vi.mocked(getVirtualServiceRules);

describe("reconcile", () => {
  let dir: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "reconcile-"));
    listMock.mockReset();
    uninstallMock.mockReset().mockResolvedValue(undefined);
    patchMock.mockReset().mockResolvedValue(undefined);
    depMock.mockReset();
    vsMock.mockReset();
  });

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  it("clean state + clean cluster: no-op", async () => {
    listMock.mockResolvedValue([]);
    depMock.mockResolvedValue({ ready: 0, total: 0, exists: false });
    vsMock.mockResolvedValue({ hasHeaderRule: false, ruleNames: ["default"] });
    const r = await reconcile({ service: "payment-service", stateDir: dir, adopt: false });
    expect(r.action).toBe("no-op");
    expect(uninstallMock).not.toHaveBeenCalled();
    expect(patchMock).not.toHaveBeenCalled();
  });

  it("orphan release with no state: uninstalls (default behavior)", async () => {
    listMock.mockResolvedValue([{ name: "payment-service-canary", status: "deployed" }]);
    depMock.mockResolvedValue({ ready: 1, total: 1, exists: true });
    vsMock.mockResolvedValue({ hasHeaderRule: false, ruleNames: ["default"] });
    const r = await reconcile({ service: "payment-service", stateDir: dir, adopt: false });
    expect(r.action).toBe("rollback-orphan");
    expect(uninstallMock).toHaveBeenCalledWith("payment-service-canary", "services");
  });

  it("orphan release with no state and --adopt: writes state + applies header rule", async () => {
    listMock.mockResolvedValue([{ name: "payment-service-canary", status: "deployed" }]);
    depMock.mockResolvedValue({ ready: 1, total: 1, exists: true });
    vsMock.mockResolvedValue({ hasHeaderRule: false, ruleNames: ["default"] });
    const r = await reconcile({ service: "payment-service", stateDir: dir, adopt: true });
    expect(r.action).toBe("adopt");
    expect(patchMock).toHaveBeenCalledWith("payment-service", "services", "HEADER:payment-service");
    expect(readState(dir, "payment-service")?.phase).toBe("active");
  });

  it("phase=deployment-ready + release Ready + no header rule: applies header rule, sets active", async () => {
    writeState(dir, { service: "order-service", phase: "deployment-ready", tag: "v2", deployedAt: "2026-05-09T18:30:00Z" });
    listMock.mockResolvedValue([{ name: "order-service-canary", status: "deployed" }]);
    depMock.mockResolvedValue({ ready: 1, total: 1, exists: true });
    vsMock.mockResolvedValue({ hasHeaderRule: false, ruleNames: ["default"] });
    const r = await reconcile({ service: "order-service", stateDir: dir, adopt: false });
    expect(r.action).toBe("complete-deploy");
    expect(patchMock).toHaveBeenCalledWith("order-service", "services", "HEADER:order-service");
    expect(readState(dir, "order-service")?.phase).toBe("active");
  });

  it("phase=active + release missing: removes header rule + clears state", async () => {
    writeState(dir, { service: "audit-service", phase: "active", tag: "v1", deployedAt: "2026-05-09T18:30:00Z" });
    listMock.mockResolvedValue([]);
    depMock.mockResolvedValue({ ready: 0, total: 0, exists: false });
    vsMock.mockResolvedValue({ hasHeaderRule: true, ruleNames: ["canary-by-header", "default"] });
    const r = await reconcile({ service: "audit-service", stateDir: dir, adopt: false });
    expect(r.action).toBe("rollback-stale-state");
    expect(patchMock).toHaveBeenCalledWith("audit-service", "services", "DEFAULT:audit-service");
    expect(readState(dir, "audit-service")).toBeNull();
  });

  it("phase=rolling-back: finishes rollback", async () => {
    writeState(dir, { service: "inventory-service", phase: "rolling-back", tag: "v1", deployedAt: "2026-05-09T18:30:00Z" });
    listMock.mockResolvedValue([{ name: "inventory-service-canary", status: "deployed" }]);
    depMock.mockResolvedValue({ ready: 1, total: 1, exists: true });
    vsMock.mockResolvedValue({ hasHeaderRule: true, ruleNames: ["canary-by-header", "default"] });
    const r = await reconcile({ service: "inventory-service", stateDir: dir, adopt: false });
    expect(r.action).toBe("finish-rollback");
    expect(patchMock).toHaveBeenCalledWith("inventory-service", "services", "DEFAULT:inventory-service");
    expect(uninstallMock).toHaveBeenCalled();
    expect(readState(dir, "inventory-service")).toBeNull();
  });

  it("phase=deploying + release NotReady: waits then rolls back if still NotReady (small timeout)", async () => {
    writeState(dir, { service: "order-service", phase: "deploying", tag: "v2", deployedAt: "2026-05-09T18:30:00Z" });
    listMock.mockResolvedValue([{ name: "order-service-canary", status: "deployed" }]);
    // Always NotReady.
    depMock.mockResolvedValue({ ready: 0, total: 1, exists: true });
    vsMock.mockResolvedValue({ hasHeaderRule: false, ruleNames: ["default"] });
    const r = await reconcile({ service: "order-service", stateDir: dir, adopt: false, reconcileTimeoutMs: 100 });
    expect(r.action).toBe("rollback-stale-state");
    expect(uninstallMock).toHaveBeenCalled();
    expect(readState(dir, "order-service")).toBeNull();
  });
});
