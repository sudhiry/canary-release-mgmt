export interface AppConfig {
  HTTP_PORT: number;
  KAFKA_BOOTSTRAP_SERVERS: string[];
  KAFKA_CONSUMERS_ENABLED: boolean;
  KAFKA_PRODUCER_ENABLED: boolean;
  KAFKA_HEALTH_TIMEOUT_MS: number;
  RESTATE_INGRESS_URL: string;
  RESTATE_REGISTER_HANDLERS: boolean;
  RESTATE_HANDLER_PORT: number;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): AppConfig {
  return {
    HTTP_PORT: Number(env.PORT ?? 3002),
    KAFKA_BOOTSTRAP_SERVERS: (env.KAFKA_BOOTSTRAP_SERVERS ?? "localhost:9092").split(","),
    KAFKA_CONSUMERS_ENABLED: env.KAFKA_CONSUMERS_ENABLED !== "false",
    KAFKA_PRODUCER_ENABLED: env.KAFKA_PRODUCER_ENABLED !== "false",
    KAFKA_HEALTH_TIMEOUT_MS: Number(env.KAFKA_HEALTH_TIMEOUT_MS ?? 30000),
    RESTATE_INGRESS_URL: env.RESTATE_INGRESS_URL ?? "http://localhost:9070",
    RESTATE_REGISTER_HANDLERS: env.RESTATE_REGISTER_HANDLERS !== "false",
    RESTATE_HANDLER_PORT: Number(env.RESTATE_HANDLER_PORT ?? 9085),
  };
}
