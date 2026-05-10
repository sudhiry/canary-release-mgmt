import { describe, it, expect } from "vitest";
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
    expect(cfg.KAFKA_HEALTH_TIMEOUT_MS).toBe(30000);
    expect(cfg.RESTATE_REGISTER_HANDLERS).toBe(true);
  });

  it("respects explicit overrides", () => {
    const cfg = loadConfig({
      INVENTORY_URL: "http://inventory.svc:8080",
      PAYMENT_URL: "http://payment.svc:8080",
      NOTIFICATION_URL: "http://notification.svc:8080",
      KAFKA_CONSUMERS_ENABLED: "false",
      KAFKA_PRODUCER_ENABLED: "false",
      KAFKA_HEALTH_TIMEOUT_MS: "5000",
      RESTATE_REGISTER_HANDLERS: "false",
    });
    expect(cfg.INVENTORY_URL).toBe("http://inventory.svc:8080");
    expect(cfg.PAYMENT_URL).toBe("http://payment.svc:8080");
    expect(cfg.NOTIFICATION_URL).toBe("http://notification.svc:8080");
    expect(cfg.KAFKA_CONSUMERS_ENABLED).toBe(false);
    expect(cfg.KAFKA_PRODUCER_ENABLED).toBe(false);
    expect(cfg.KAFKA_HEALTH_TIMEOUT_MS).toBe(5000);
    expect(cfg.RESTATE_REGISTER_HANDLERS).toBe(false);
  });
});
