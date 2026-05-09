import { loadConfig } from "./config.js";

const config = loadConfig();

console.log("notification-service booting", {
  httpPort: config.HTTP_PORT,
  restateRegisterHandlers: config.RESTATE_REGISTER_HANDLERS,
  kafkaConsumersEnabled: config.KAFKA_CONSUMERS_ENABLED,
});

// HTTP, Kafka, Restate setup are wired in Tasks 17, 18, 19.
