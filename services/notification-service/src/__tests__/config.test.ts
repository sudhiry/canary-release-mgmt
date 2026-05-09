import { describe, it, expect } from "vitest";
import { loadConfig } from "../config.js";

describe("loadConfig", () => {
  it("uses defaults when env is empty", () => {
    const cfg = loadConfig({});
    expect(cfg.HTTP_PORT).toBe(3002);
    expect(cfg.KAFKA_BOOTSTRAP_SERVERS).toEqual(["localhost:9092"]);
    expect(cfg.KAFKA_CONSUMERS_ENABLED).toBe(true);
    expect(cfg.KAFKA_PRODUCER_ENABLED).toBe(true);
    expect(cfg.RESTATE_REGISTER_HANDLERS).toBe(true);
    expect(cfg.RESTATE_INGRESS_URL).toBe("http://localhost:9070");
    expect(cfg.RESTATE_HANDLER_PORT).toBe(9085);
  });

  it("treats KAFKA_CONSUMERS_ENABLED=false as false", () => {
    const cfg = loadConfig({ KAFKA_CONSUMERS_ENABLED: "false" });
    expect(cfg.KAFKA_CONSUMERS_ENABLED).toBe(false);
  });

  it("treats KAFKA_PRODUCER_ENABLED=false as false", () => {
    const cfg = loadConfig({ KAFKA_PRODUCER_ENABLED: "false" });
    expect(cfg.KAFKA_PRODUCER_ENABLED).toBe(false);
  });

  it("treats RESTATE_REGISTER_HANDLERS=false as false", () => {
    const cfg = loadConfig({ RESTATE_REGISTER_HANDLERS: "false" });
    expect(cfg.RESTATE_REGISTER_HANDLERS).toBe(false);
  });

  it("treats any other value as true (unset → enabled)", () => {
    expect(loadConfig({ KAFKA_CONSUMERS_ENABLED: "anything" }).KAFKA_CONSUMERS_ENABLED).toBe(true);
    expect(loadConfig({}).KAFKA_CONSUMERS_ENABLED).toBe(true);
  });

  it("splits KAFKA_BOOTSTRAP_SERVERS on commas", () => {
    const cfg = loadConfig({ KAFKA_BOOTSTRAP_SERVERS: "a:9092,b:9092,c:9092" });
    expect(cfg.KAFKA_BOOTSTRAP_SERVERS).toEqual(["a:9092", "b:9092", "c:9092"]);
  });
});
