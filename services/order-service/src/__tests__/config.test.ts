import { describe, it, expect, vi } from "vitest";
import { loadConfig } from "../config.js";

describe("loadConfig", () => {
  it("uses defaults when env is empty", () => {
    const cfg = loadConfig({});
    expect(cfg.HTTP_PORT).toBe(3001);
    expect(cfg.RESTATE_HANDLER_PORT).toBe(9084);
    expect(cfg.INVENTORY_URL).toBe("http://localhost:8082");
    expect(cfg.PAYMENT_URL).toBe("http://localhost:8081");
    expect(cfg.NOTIFICATION_URL).toBe("http://localhost:3002");
    expect(cfg.KAFKA_CONSUMERS_ENABLED).toBe(true);
    expect(cfg.KAFKA_PRODUCER_ENABLED).toBe(true);
    expect(cfg.RESTATE_REGISTER_HANDLERS).toBe(true);
  });

  it("respects explicit overrides", () => {
    const cfg = loadConfig({
      INVENTORY_URL: "http://inventory.svc:8080",
      PAYMENT_URL: "http://payment.svc:8080",
      NOTIFICATION_URL: "http://notification.svc:8080",
      KAFKA_CONSUMERS_ENABLED: "false",
      KAFKA_PRODUCER_ENABLED: "false",
      RESTATE_REGISTER_HANDLERS: "false",
    });
    expect(cfg.INVENTORY_URL).toBe("http://inventory.svc:8080");
    expect(cfg.PAYMENT_URL).toBe("http://payment.svc:8080");
    expect(cfg.NOTIFICATION_URL).toBe("http://notification.svc:8080");
    expect(cfg.KAFKA_CONSUMERS_ENABLED).toBe(false);
    expect(cfg.KAFKA_PRODUCER_ENABLED).toBe(false);
    expect(cfg.RESTATE_REGISTER_HANDLERS).toBe(false);
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
