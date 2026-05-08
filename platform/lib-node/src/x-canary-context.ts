import { AsyncLocalStorage } from "node:async_hooks";

const storage = new AsyncLocalStorage<{ canary: boolean }>();

export function isCanary(): boolean {
  return storage.getStore()?.canary === true;
}

export function setCanary(canary: boolean): void {
  // Mutates the current store frame (if any). When no frame exists, this is a no-op
  // — runWithCanary is the proper way to enter a context.
  const store = storage.getStore();
  if (store) {
    store.canary = canary;
  } else {
    storage.enterWith({ canary });
  }
}

export function clearCanary(): void {
  const store = storage.getStore();
  if (store) {
    store.canary = false;
  }
}

export async function runWithCanary<T>(canary: boolean, fn: () => Promise<T> | T): Promise<T> {
  return storage.run({ canary }, async () => fn());
}
