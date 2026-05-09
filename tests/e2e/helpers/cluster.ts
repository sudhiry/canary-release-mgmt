import { rollback } from "./canary.js";

const ALL_SERVICES = [
  "order-service",
  "payment-service",
  "inventory-service",
  "notification-service",
  "audit-service",
] as const;

/**
 * Idempotent: rolls back any leftover canary on every Phase 1 service.
 * Used in scenario beforeAll hooks to guarantee a known starting state.
 */
export async function ensureCleanBaseline(): Promise<void> {
  for (const svc of ALL_SERVICES) {
    await rollback(svc);
  }
}

export const PHASE1_SERVICES = ALL_SERVICES;
