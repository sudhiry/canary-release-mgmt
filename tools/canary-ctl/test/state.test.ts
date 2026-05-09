import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { mkdtempSync, rmSync, existsSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { type CanaryState, readState, writeState, deleteState } from "../src/state.js";

describe("state", () => {
  let dir: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "canary-ctl-state-"));
  });

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  it("readState returns null when the file is absent", () => {
    expect(readState(dir, "payment-service")).toBeNull();
  });

  it("writeState then readState roundtrips", () => {
    const s: CanaryState = {
      service: "payment-service",
      phase: "active",
      tag: "v2",
      deployedAt: "2026-05-09T18:30:00Z",
    };
    writeState(dir, s);
    expect(readState(dir, "payment-service")).toEqual(s);
  });

  it("writeState is atomic (no partial file on rename)", () => {
    const s: CanaryState = {
      service: "order-service",
      phase: "deploying",
      tag: "v1",
      deployedAt: "2026-05-09T18:30:00Z",
    };
    writeState(dir, s);
    const path = join(dir, "order-service.json");
    expect(existsSync(path)).toBe(true);
    expect(existsSync(`${path}.tmp`)).toBe(false);
  });

  it("deleteState removes the file (idempotent if absent)", () => {
    const s: CanaryState = {
      service: "audit-service",
      phase: "active",
      tag: "v1",
      deployedAt: "2026-05-09T18:30:00Z",
    };
    writeState(dir, s);
    deleteState(dir, "audit-service");
    expect(readState(dir, "audit-service")).toBeNull();
    // Idempotent: second call does not throw.
    expect(() => deleteState(dir, "audit-service")).not.toThrow();
  });

  it("readState throws a clear error on invalid JSON", () => {
    const path = join(dir, "broken.json");
    writeFileSync(path, "{ this is not json", "utf8");
    expect(() => readState(dir, "broken")).toThrow(/invalid state file/i);
  });

  it("each phase value roundtrips", () => {
    for (const phase of ["deploying", "deployment-ready", "active", "rolling-back"] as const) {
      const s: CanaryState = {
        service: "inventory-service",
        phase,
        tag: "v1",
        deployedAt: "2026-05-09T18:30:00Z",
      };
      writeState(dir, s);
      expect(readState(dir, "inventory-service")?.phase).toBe(phase);
    }
  });
});
