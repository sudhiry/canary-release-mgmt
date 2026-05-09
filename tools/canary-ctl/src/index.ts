import { Command } from "commander";
import { homedir } from "node:os";
import { join } from "node:path";
import { setVerbose } from "./logger.js";
import { deployCanary } from "./commands/deploy-canary.js";
import { rollback } from "./commands/rollback.js";
import { computeStatus, formatStatusText } from "./commands/status.js";
import { reconcile } from "./commands/reconcile.js";

interface GlobalOpts {
  stateDir: string;
  repoRoot: string;
  graceSeconds: number;
  verbose: boolean;
}

function makeProgram(): Command {
  const program = new Command();
  program
    .name("canary-ctl")
    .description("Manage per-service canary lifecycle (Helm release + VirtualService header rule + state file)")
    .option("--state-dir <path>", "directory for per-service state files", join(homedir(), ".canary-ctl"))
    .option("--repo-root <path>", "repo root for resolving Helm chart and values paths", process.cwd())
    .option("--grace-seconds <n>", "rollback grace period (sec)", (v) => parseInt(v, 10), 10)
    .option("--verbose", "enable debug logging", false);

  program.command("deploy-canary <service> <tag>")
    .description("Create canary release and apply VS header-match rule")
    .action(async (service: string, tag: string) => {
      const o = program.opts<GlobalOpts>();
      setVerbose(o.verbose);
      await deployCanary({ service, tag, stateDir: o.stateDir, repoRoot: o.repoRoot });
    });

  program.command("rollback <service>")
    .description("Remove header rule, drain, uninstall canary release, clear state")
    .action(async (service: string) => {
      const o = program.opts<GlobalOpts>();
      setVerbose(o.verbose);
      await rollback({ service, stateDir: o.stateDir, graceSeconds: o.graceSeconds });
    });

  program.command("status <service>")
    .description("Print canary state for a service (text or --json)")
    .option("--json", "machine-readable output", false)
    .action(async (service: string, cmdOpts: { json: boolean }) => {
      const o = program.opts<GlobalOpts>();
      setVerbose(o.verbose);
      const s = await computeStatus({ service, stateDir: o.stateDir });
      if (cmdOpts.json) {
        process.stdout.write(JSON.stringify(s, null, 2) + "\n");
      } else {
        process.stdout.write(formatStatusText(s) + "\n");
      }
      if (s.drift.length > 0) process.exit(2);
    });

  program.command("reconcile <service>")
    .description("Inspect cluster + state and bring them to a consistent state")
    .option("--adopt", "adopt orphan canary releases instead of rolling back", false)
    .action(async (service: string, cmdOpts: { adopt: boolean }) => {
      const o = program.opts<GlobalOpts>();
      setVerbose(o.verbose);
      const r = await reconcile({ service, stateDir: o.stateDir, adopt: cmdOpts.adopt });
      process.stdout.write(`reconcile: ${r.action}\n`);
    });

  return program;
}

export async function run(argv: string[]): Promise<void> {
  const program = makeProgram();
  try {
    await program.parseAsync(argv);
  } catch (err) {
    process.stderr.write(`canary-ctl: ${(err as Error).message}\n`);
    process.exit(1);
  }
}
