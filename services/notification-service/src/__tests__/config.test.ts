import { describe, it, expect, vi } from "vitest";
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

  it("defaults KAFKA_HEARTBEAT_STALE_MS to 15000", () => {
    const cfg = loadConfig({});
    expect(cfg.KAFKA_HEARTBEAT_STALE_MS).toBe(15000);
  });

  it("respects KAFKA_HEARTBEAT_STALE_MS override", () => {
    const cfg = loadConfig({ KAFKA_HEARTBEAT_STALE_MS: "5000" });
    expect(cfg.KAFKA_HEARTBEAT_STALE_MS).toBe(5000);
  });

  it("honors deprecated KAFKA_HEALTH_TIMEOUT_MS with warn", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const cfg = loadConfig({ KAFKA_HEALTH_TIMEOUT_MS: "7000" });
    expect(cfg.KAFKA_HEARTBEAT_STALE_MS).toBe(7000);
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("deprecated"));
    expect(warn).toHaveBeenCalledTimes(1);
    warn.mockRestore();
  });

  it("new var wins over deprecated alias", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const cfg = loadConfig({
      KAFKA_HEARTBEAT_STALE_MS: "1000",
      KAFKA_HEALTH_TIMEOUT_MS: "9999",
    });
    expect(cfg.KAFKA_HEARTBEAT_STALE_MS).toBe(1000);
    expect(warn).not.toHaveBeenCalled();
    warn.mockRestore();
  });

  it("treats empty string KAFKA_HEARTBEAT_STALE_MS as unset", () => {
    const cfg = loadConfig({ KAFKA_HEARTBEAT_STALE_MS: "" });
    expect(cfg.KAFKA_HEARTBEAT_STALE_MS).toBe(15000);
  });
});
