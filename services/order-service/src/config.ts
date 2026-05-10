export interface AppConfig {
  HTTP_PORT: number;
  KAFKA_BOOTSTRAP_SERVERS: string[];
  KAFKA_CONSUMERS_ENABLED: boolean;
  KAFKA_PRODUCER_ENABLED: boolean;
  KAFKA_HEARTBEAT_STALE_MS: number;
  RESTATE_INGRESS_URL: string;
  RESTATE_REGISTER_HANDLERS: boolean;
  RESTATE_HANDLER_PORT: number;
  INVENTORY_URL: string;
  PAYMENT_URL: string;
  NOTIFICATION_URL: string;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  return {
    HTTP_PORT: Number(env.PORT ?? 3001),
    KAFKA_BOOTSTRAP_SERVERS: (env.KAFKA_BOOTSTRAP_SERVERS ?? "localhost:9092").split(","),
    KAFKA_CONSUMERS_ENABLED: env.KAFKA_CONSUMERS_ENABLED !== "false",
    KAFKA_PRODUCER_ENABLED: env.KAFKA_PRODUCER_ENABLED !== "false",
    KAFKA_HEARTBEAT_STALE_MS: (() => {
      if (env.KAFKA_HEARTBEAT_STALE_MS !== undefined) {
        return Number(env.KAFKA_HEARTBEAT_STALE_MS);
      }
      if (env.KAFKA_HEALTH_TIMEOUT_MS !== undefined) {
        console.warn(
          "KAFKA_HEALTH_TIMEOUT_MS is deprecated; use KAFKA_HEARTBEAT_STALE_MS",
        );
        return Number(env.KAFKA_HEALTH_TIMEOUT_MS);
      }
      return 15000;
    })(),
    RESTATE_INGRESS_URL: env.RESTATE_INGRESS_URL ?? "http://localhost:9070",
    RESTATE_REGISTER_HANDLERS: env.RESTATE_REGISTER_HANDLERS !== "false",
    RESTATE_HANDLER_PORT: Number(env.RESTATE_HANDLER_PORT ?? 9084),
    INVENTORY_URL: env.INVENTORY_URL ?? "http://localhost:8082",
    PAYMENT_URL: env.PAYMENT_URL ?? "http://localhost:8081",
    NOTIFICATION_URL: env.NOTIFICATION_URL ?? "http://localhost:3002",
  };
}
