import { describe, expect, it, vi } from "vitest";

vi.mock("../src/exec.js", () => ({
  run: vi.fn(async () => ({ stdout: "[]", stderr: "" })),
}));

import { run } from "../src/exec.js";
import { upgradeInstallCanary, uninstallCanary, listReleases } from "../src/helm.js";

const runMock = vi.mocked(run);

describe("helm shell-outs", () => {
  it("upgradeInstallCanary passes both values files and the image tag", async () => {
    runMock.mockResolvedValue({ stdout: "", stderr: "" });
    await upgradeInstallCanary({
      releaseName: "payment-service-canary",
      namespace: "services",
      chartPath: "deploy/helm/service-chart",
      valuesFile: "deploy/helm/values/payment-service.yaml",
      canaryOverlay: "deploy/helm/values/canary-overlay.yaml",
      tag: "v2",
      timeoutSeconds: 120,
    });
    expect(runMock).toHaveBeenCalledWith(
      "helm",
      [
        "upgrade", "--install", "payment-service-canary",
        "deploy/helm/service-chart",
        "-n", "services",
        "-f", "deploy/helm/values/payment-service.yaml",
        "-f", "deploy/helm/values/canary-overlay.yaml",
        "--set", "image.tag=v2",
        "--wait",
        "--timeout", "120s",
      ],
      expect.any(Object),
    );
  });

  it("uninstallCanary runs helm uninstall --wait", async () => {
    runMock.mockResolvedValue({ stdout: "", stderr: "" });
    await uninstallCanary("order-service-canary", "services");
    expect(runMock).toHaveBeenCalledWith(
      "helm",
      ["uninstall", "order-service-canary", "-n", "services", "--wait"],
      expect.any(Object),
    );
  });

  it("listReleases returns parsed JSON array", async () => {
    runMock.mockResolvedValue({
      stdout: JSON.stringify([
        { name: "payment-service", status: "deployed" },
        { name: "payment-service-canary", status: "deployed" },
      ]),
      stderr: "",
    });
    const releases = await listReleases("services");
    expect(releases.map((r) => r.name)).toEqual([
      "payment-service",
      "payment-service-canary",
    ]);
  });
});
