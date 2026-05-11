import "./tracing.js";   // MUST be first — initializes OTel SDK before express/kafkajs load
import axios from "axios";
import {
  attachXCanaryAxiosInterceptor,
  attachXServedChainAxiosInterceptor,
  CanaryMetrics,
  canaryMetricsEndpoint,
  LaneStateProbe,
} from "@canary/lib-node";
import { loadConfig } from "./config.js";
import { setupHttp } from "./http.js";
import { setupKafka } from "./kafka.js";
import { setupRestate } from "./restate.js";

const config = loadConfig();

const metrics = new CanaryMetrics("order");
const laneProbe = new LaneStateProbe(
  process.env.POD_NAMESPACE ?? "services",
  "order",
);
laneProbe.registerGauges();
void laneProbe.start();

const ingressClient = axios.create({ baseURL: config.RESTATE_INGRESS_URL });
attachXCanaryAxiosInterceptor(ingressClient);
attachXServedChainAxiosInterceptor(ingressClient);

const kafka = await setupKafka({
  brokers: config.KAFKA_BOOTSTRAP_SERVERS,
  consumersEnabled: config.KAFKA_CONSUMERS_ENABLED,
  producerEnabled: config.KAFKA_PRODUCER_ENABLED,
  heartbeatStaleMs: config.KAFKA_HEARTBEAT_STALE_MS,
  metrics,
});

const app = setupHttp({
  ingressClient,
  kafkaSend: kafka.send,
  kafkaHealth: kafka.health,
  version: process.env.VERSION ?? "stable",
  metrics,
});
app.get("/actuator/prometheus", canaryMetricsEndpoint(metrics));

const server = app.listen(config.HTTP_PORT, () => {
  console.log(`order-service HTTP listening on ${config.HTTP_PORT}`);
});

await setupRestate({
  registerHandlers: config.RESTATE_REGISTER_HANDLERS,
  port: config.RESTATE_HANDLER_PORT,
  metrics,
});

const shutdown = async () => {
  console.log("order-service shutting down");
  laneProbe.close();
  if (kafka.presenceWatcher) kafka.presenceWatcher.close();
  if (kafka.consumer) await kafka.consumer.disconnect().catch(() => {});
  if (kafka.producer) await kafka.producer.disconnect().catch(() => {});
  server.close();
};
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
