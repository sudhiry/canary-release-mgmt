import { loadConfig } from "./config.js";
import { setupHttp, buildIngressClient } from "./http.js";
import { setupKafka } from "./kafka.js";
import { setupRestate, configureKafkaSend } from "./restate.js";

const config = loadConfig();

const ingressClient = buildIngressClient(config.RESTATE_INGRESS_URL);

const kafka = await setupKafka({
  brokers: config.KAFKA_BOOTSTRAP_SERVERS,
  consumersEnabled: config.KAFKA_CONSUMERS_ENABLED,
  producerEnabled: config.KAFKA_PRODUCER_ENABLED,
});

const app = setupHttp({ ingressClient, kafkaHealth: kafka.health });

const server = app.listen(config.HTTP_PORT, () => {
  console.log(`notification-service HTTP listening on ${config.HTTP_PORT}`);
});

configureKafkaSend(kafka.send);

await setupRestate({
  registerHandlers: config.RESTATE_REGISTER_HANDLERS,
  port: config.RESTATE_HANDLER_PORT,
});

const shutdown = async () => {
  console.log("notification-service shutting down");
  if (kafka.presenceWatcher) kafka.presenceWatcher.close();
  if (kafka.consumer) await kafka.consumer.disconnect().catch(() => {});
  if (kafka.producer) await kafka.producer.disconnect().catch(() => {});
  server.close();
};
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
