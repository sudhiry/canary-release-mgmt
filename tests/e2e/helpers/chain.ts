export interface ChainEntry {
  service: string;
  version: string;
}

export const X_SERVED_CHAIN_HEADER = "x-served-chain";

export function parseChain(raw: string | string[] | undefined | null): ChainEntry[] {
  if (typeof raw !== "string") {
    if (Array.isArray(raw)) return parseChain(raw.join(","));
    return [];
  }
  const trimmed = raw.trim();
  if (trimmed.length === 0) return [];
  const out: ChainEntry[] = [];
  for (const token of trimmed.split(",")) {
    const t = token.trim();
    if (!t) continue;
    const eq = t.indexOf("=");
    if (eq <= 0) continue;
    out.push({ service: t.slice(0, eq), version: t.slice(eq + 1) });
  }
  return out;
}

export function getChain(headers: Record<string, string | string[] | undefined>): ChainEntry[] {
  return parseChain(headers[X_SERVED_CHAIN_HEADER]);
}

export function assertVersion(chain: ChainEntry[], service: string, expected: string): void {
  const matches = chain.filter((e) => e.service === service);
  if (matches.length === 0) {
    throw new Error(
      `${service}: not present in chain (chain: ${chain.map((e) => `${e.service}=${e.version}`).join(",") || "<empty>"})`,
    );
  }
  for (const m of matches) {
    if (m.version !== expected) {
      throw new Error(
        `${service}: expected ${expected}, got ${m.version} (chain: ${chain.map((e) => `${e.service}=${e.version}`).join(",")})`,
      );
    }
  }
}

export function assertVersions(chain: ChainEntry[], expectations: Record<string, string>): void {
  for (const [service, expected] of Object.entries(expectations)) {
    assertVersion(chain, service, expected);
  }
}

export function assertContains(chain: ChainEntry[], service: string): void {
  if (!chain.some((e) => e.service === service)) {
    throw new Error(
      `${service}: not present in chain (chain: ${chain.map((e) => `${e.service}=${e.version}`).join(",") || "<empty>"})`,
    );
  }
}

export function assertAbsent(chain: ChainEntry[], service: string): void {
  if (chain.some((e) => e.service === service)) {
    throw new Error(
      `${service}: unexpectedly present in chain (chain: ${chain.map((e) => `${e.service}=${e.version}`).join(",")})`,
    );
  }
}
