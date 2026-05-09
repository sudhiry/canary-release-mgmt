import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

vi.mock("../src/helm.js", () => ({ listReleases: vi.fn() }));
vi.mock("../src/kubectl.js", () => ({
  getDeploymentReady: vi.fn(),
  getVirtualServiceRules: vi.fn(),
}));

import { listReleases } from "../src/helm.js";
import { getDeploymentReady, getVirtualServiceRules } from "../src/kubectl.js";
import { writeState } from "../src/state.js";
import { computeStatus } from "../src/commands/status.js";

const listMock = vi.mocked(listReleases);
const deployMock = vi.mocked(getDeploymentReady);
const vsMock = vi.mocked(getVirtualServiceRules);

describe("status", () => {
  let dir: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "status-"));
    listMock.mockReset();
    deployMock.mockReset();
    vsMock.mockReset();
  });

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  it("clean cluster, no state: drift=none, canary=absent", async () => {
    listMock.mockResolvedValue([{ name: "payment-service", status: "deployed" }]);
    deployMock.mockResolvedValue({ ready: 0, total: 0, exists: false });
    vsMock.mockResolvedValue({ hasHeaderRule: false, ruleNames: ["default"] });
    const s = await computeStatus({ service: "payment-service", stateDir: dir });
    expect(s.statePhase).toBeNull();
    expect(s.helmCanaryPresent).toBe(false);
    expect(s.vsHasHeaderRule).toBe(false);
    expect(s.drift).toEqual([]);
  });

  it("active state with all cluster pieces present: drift=none", async () => {
    writeState(dir, { service: "payment-service", phase: "active", tag: "v2", deployedAt: "2026-05-09T18:30:00Z" });
    listMock.mockResolvedValue([
      { name: "payment-service", status: "deployed" },
      { name: "payment-service-canary", status: "deployed" },
    ]);
    deployMock.mockResolvedValue({ ready: 1, total: 1, exists: true });
    vsMock.mockResolvedValue({ hasHeaderRule: true, ruleNames: ["canary-by-header", "default"] });
    const s = await computeStatus({ service: "payment-service", stateDir: dir });
    expect(s.statePhase).toBe("active");
    expect(s.drift).toEqual([]);
  });

  it("state says active but VS lacks header rule: drift detected", async () => {
    writeState(dir, { service: "order-service", phase: "active", tag: "v2", deployedAt: "2026-05-09T18:30:00Z" });
    listMock.mockResolvedValue([
      { name: "order-service", status: "deployed" },
      { name: "order-service-canary", status: "deployed" },
    ]);
    deployMock.mockResolvedValue({ ready: 1, total: 1, exists: true });
    vsMock.mockResolvedValue({ hasHeaderRule: false, ruleNames: ["default"] });
    const s = await computeStatus({ service: "order-service", stateDir: dir });
    expect(s.drift).toContain("state phase=active but VS has no header rule");
  });

  it("no state but canary release present: drift detected", async () => {
    listMock.mockResolvedValue([
      { name: "audit-service", status: "deployed" },
      { name: "audit-service-canary", status: "deployed" },
    ]);
    deployMock.mockResolvedValue({ ready: 1, total: 1, exists: true });
    vsMock.mockResolvedValue({ hasHeaderRule: false, ruleNames: ["default"] });
    const s = await computeStatus({ service: "audit-service", stateDir: dir });
    expect(s.drift).toContain("state file missing but canary release present");
  });
});
