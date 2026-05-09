type Level = "info" | "warn" | "error" | "debug";

let verbose = false;

export function setVerbose(v: boolean): void {
  verbose = v;
}

export function log(level: Level, msg: string, fields?: Record<string, unknown>): void {
  if (level === "debug" && !verbose) return;
  const stamp = new Date().toISOString();
  const f = fields ? " " + Object.entries(fields).map(([k, v]) => `${k}=${JSON.stringify(v)}`).join(" ") : "";
  // Always to stderr so stdout stays clean for `--json` outputs.
  process.stderr.write(`${stamp} [${level}] ${msg}${f}\n`);
}
