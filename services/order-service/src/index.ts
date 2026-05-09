import { loadConfig } from "./config.js";
import { setupHttp, buildClient } from "./http.js";
import { setupKafka } from "./kafka.js";
import { setupRestate } from "./restate.js";

const config = loadConfig();

const clients = {
  inventory: buildClient(config.INVENTORY_URL),
  payment: buildClient(config.PAYMENT_URL),
  notification: buildClient(config.NOTIFICATION_URL),
};

const kafka = await setupKafka({
  brokers: config.KAFKA_BOOTSTRAP_SERVERS,
  consumersEnabled: config.KAFKA_CONSUMERS_ENABLED,
  producerEnabled: config.KAFKA_PRODUCER_ENABLED,
});

const app = setupHttp({ clients, kafkaSend: kafka.send, kafkaHealth: kafka.health });

const server = app.listen(config.HTTP_PORT, () => {
  console.log(`order-service HTTP listening on ${config.HTTP_PORT}`);
});

await setupRestate({
  registerHandlers: config.RESTATE_REGISTER_HANDLERS,
  port: config.RESTATE_HANDLER_PORT,
});

const shutdown = async () => {
  console.log("order-service shutting down");
  if (kafka.presenceWatcher) { try { kafka.presenceWatcher.close(); } catch { /* ignore */ } }
  if (kafka.consumer) await kafka.consumer.disconnect().catch(() => {});
  if (kafka.producer) await kafka.producer.disconnect().catch(() => {});
  server.close();
};
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
