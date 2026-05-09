import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

vi.mock("../src/helm.js", () => ({
  uninstallCanary: vi.fn(async () => {}),
  listReleases: vi.fn(async () => [] as Array<{ name: string; status: string }>),
}));
vi.mock("../src/kubectl.js", () => ({
  patchVirtualService: vi.fn(async () => {}),
  buildDefaultOnlyPatch: (svc: string) => `DEFAULT:${svc}`,
  getVirtualServiceRules: vi.fn(),
}));

import { uninstallCanary, listReleases } from "../src/helm.js";
import { patchVirtualService, getVirtualServiceRules } from "../src/kubectl.js";
import { writeState, readState } from "../src/state.js";
import { rollback } from "../src/commands/rollback.js";

const uninstallMock = vi.mocked(uninstallCanary);
const patchMock = vi.mocked(patchVirtualService);
const listMock = vi.mocked(listReleases);
const getVsMock = vi.mocked(getVirtualServiceRules);

describe("rollback", () => {
  let dir: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "rollback-"));
    uninstallMock.mockReset().mockResolvedValue(undefined);
    patchMock.mockReset().mockResolvedValue(undefined);
    listMock.mockReset().mockResolvedValue([]);
    getVsMock.mockReset().mockResolvedValue({ hasHeaderRule: false, ruleNames: ["default"] });
  });

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  it("active state: header rule first, then helm uninstall, then state cleared", async () => {
    writeState(dir, {
      service: "payment-service",
      phase: "active",
      tag: "v2",
      deployedAt: "2026-05-09T18:30:00Z",
    });
    listMock.mockResolvedValue([{ name: "payment-service-canary", status: "deployed" }]);

    await rollback({ service: "payment-service", stateDir: dir, graceSeconds: 0 });

    // Order: patch was called BEFORE uninstall.
    expect(patchMock).toHaveBeenCalledWith("payment-service", "services", "DEFAULT:payment-service");
    expect(uninstallMock).toHaveBeenCalledWith("payment-service-canary", "services");
    expect(patchMock.mock.invocationCallOrder[0]).toBeLessThan(uninstallMock.mock.invocationCallOrder[0]);
    expect(readState(dir, "payment-service")).toBeNull();
  });

  it("absent state with no canary release: no-op, exits clean", async () => {
    listMock.mockResolvedValue([{ name: "payment-service", status: "deployed" }]); // only stable
    await rollback({ service: "payment-service", stateDir: dir, graceSeconds: 0 });
    expect(uninstallMock).not.toHaveBeenCalled();
    expect(patchMock).not.toHaveBeenCalled();
  });

  it("absent state but orphan release: uninstall it", async () => {
    listMock.mockResolvedValue([
      { name: "payment-service", status: "deployed" },
      { name: "payment-service-canary", status: "deployed" },
    ]);
    await rollback({ service: "payment-service", stateDir: dir, graceSeconds: 0 });
    expect(uninstallMock).toHaveBeenCalledWith("payment-service-canary", "services");
  });

  it("active state but VS already lacks header rule: still uninstall, still idempotent", async () => {
    writeState(dir, {
      service: "order-service",
      phase: "active",
      tag: "v1",
      deployedAt: "2026-05-09T18:30:00Z",
    });
    listMock.mockResolvedValue([{ name: "order-service-canary", status: "deployed" }]);
    getVsMock.mockResolvedValue({ hasHeaderRule: false, ruleNames: ["default"] });
    await rollback({ service: "order-service", stateDir: dir, graceSeconds: 0 });
    expect(uninstallMock).toHaveBeenCalled();
    expect(readState(dir, "order-service")).toBeNull();
  });
});
