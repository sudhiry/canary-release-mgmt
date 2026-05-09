import { run } from "./exec.js";

export interface UpgradeInstallArgs {
  releaseName: string;
  namespace: string;
  chartPath: string;
  valuesFile: string;
  canaryOverlay: string;
  tag: string;
  timeoutSeconds: number;
}

export async function upgradeInstallCanary(args: UpgradeInstallArgs): Promise<void> {
  await run(
    "helm",
    [
      "upgrade", "--install", args.releaseName,
      args.chartPath,
      "-n", args.namespace,
      "-f", args.valuesFile,
      "-f", args.canaryOverlay,
      "--set", `image.tag=${args.tag}`,
      "--wait",
      "--timeout", `${args.timeoutSeconds}s`,
    ],
    { timeoutMs: (args.timeoutSeconds + 30) * 1000 },
  );
}

export async function uninstallCanary(releaseName: string, namespace: string): Promise<void> {
  await run(
    "helm",
    ["uninstall", releaseName, "-n", namespace, "--wait"],
    { timeoutMs: 120_000, ignoreError: true },
  );
}

export interface HelmRelease {
  name: string;
  status: string;
}

export async function listReleases(namespace: string): Promise<HelmRelease[]> {
  const r = await run("helm", ["list", "-n", namespace, "-o", "json"], {});
  return JSON.parse(r.stdout) as HelmRelease[];
}
