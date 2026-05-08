import { afterEach, describe, expect, it } from "vitest";
import { runWithCanary, clearCanary } from "../x-canary-context.js";
import { stampXCanaryOnProducerRecord } from "../x-canary-kafka.js";

describe("stampXCanaryOnProducerRecord", () => {
  afterEach(() => clearCanary());

  it("adds x-canary header to every message when context is canary", async () => {
    await runWithCanary(true, async () => {
      const record = {
        topic: "t",
        messages: [
          { key: "k1", value: "v1" },
          { key: "k2", value: "v2", headers: { existing: "x" } },
        ],
      };
      const stamped = stampXCanaryOnProducerRecord(record);
      expect(stamped.messages[0].headers).toMatchObject({ "x-canary": "true" });
      expect(stamped.messages[1].headers).toMatchObject({ existing: "x", "x-canary": "true" });
    });
  });

  it("does not add header when context is not canary", () => {
    const record = { topic: "t", messages: [{ key: "k", value: "v" }] };
    const stamped = stampXCanaryOnProducerRecord(record);
    expect(stamped.messages[0].headers).toBeUndefined();
  });

  it("does not overwrite existing x-canary header", async () => {
    await runWithCanary(true, async () => {
      const record = {
        topic: "t",
        messages: [{ key: "k", value: "v", headers: { "x-canary": "preset" } }],
      };
      const stamped = stampXCanaryOnProducerRecord(record);
      expect(stamped.messages[0].headers!["x-canary"]).toBe("preset");
    });
  });
});
