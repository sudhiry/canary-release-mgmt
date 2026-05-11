import type { V1Endpoints } from "@kubernetes/client-node";
import { Registry } from "prom-client";
import { describe, expect, it } from "vitest";
import { LaneStateProbe } from "../observability/lane-state-probe.js";

describe("LaneStateProbe", () => {
  it("registers gauges for both lanes at zero before any update", () => {
    const registry = new Registry();
    const probe = new LaneStateProbe("services", "payment", registry);

    probe.registerGauges();

    expect(probe.laneValue("stable")).toBe(0);
    expect(probe.laneValue("canary")).toBe(0);
  });

  it("setLaneActive toggles gauge value", () => {
    const registry = new Registry();
    const probe = new LaneStateProbe("services", "payment", registry);
    probe.registerGauges();

    probe.setLaneActive("canary", true);
    expect(probe.laneValue("canary")).toBe(1);

    probe.setLaneActive("canary", false);
    expect(probe.laneValue("canary")).toBe(0);
  });

  it("hasAddresses detects populated/empty Endpoints", () => {
    const populated: V1Endpoints = { subsets: [{ addresses: [{ ip: "10.0.0.1" }] }] };
    const empty: V1Endpoints = { subsets: [] };
    expect(LaneStateProbe.hasAddresses(populated)).toBe(true);
    expect(LaneStateProbe.hasAddresses(empty)).toBe(false);
    expect(LaneStateProbe.hasAddresses(undefined)).toBe(false);
  });

  it("gauge has substrate=http, service, lane labels", async () => {
    const registry = new Registry();
    const probe = new LaneStateProbe("services", "payment", registry);
    probe.registerGauges();
    probe.setLaneActive("stable", true);

    const g = await registry.getSingleMetric("canary_lane_active")?.get();
    expect(g?.values).toContainEqual(
      expect.objectContaining({
        value: 1,
        labels: { substrate: "http", service: "payment", lane: "stable" },
      }),
    );
  });
});
