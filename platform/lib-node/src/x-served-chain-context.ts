import { AsyncLocalStorage } from "node:async_hooks";

export const X_SERVED_CHAIN_HEADER = "x-served-chain";

interface ChainStore {
  tokens: string[];
}

const storage = new AsyncLocalStorage<ChainStore>();

export function runWithChain<T>(fn: () => T): T {
  return storage.run({ tokens: [] }, fn);
}

export function appendToken(token: string | undefined | null): void {
  const store = storage.getStore();
  if (!store) return;
  if (typeof token !== "string") return;
  const trimmed = token.trim();
  if (trimmed.length === 0) return;
  store.tokens.push(trimmed);
}

export function appendChain(chainCsv: string | undefined | null): void {
  if (typeof chainCsv !== "string") return;
  const trimmed = chainCsv.trim();
  if (trimmed.length === 0) return;
  for (const t of trimmed.split(",")) appendToken(t);
}

export function collectTokens(): string[] {
  const store = storage.getStore();
  return store ? [...store.tokens] : [];
}
