import { describe, expect, it } from "vitest";
import { runWithCanary } from "../x-canary-context.js";
import { currentLane } from "../observability/canary-lane-tag.js";

describe("currentLane", () => {
  it("returns 'stable' when no context", () => {
    expect(currentLane()).toBe("stable");
  });

  it("returns 'canary' inside a canary context", async () => {
    await runWithCanary(true, async () => {
      expect(currentLane()).toBe("canary");
    });
  });

  it("returns 'stable' inside a stable context", async () => {
    await runWithCanary(false, async () => {
      expect(currentLane()).toBe("stable");
    });
  });
});
