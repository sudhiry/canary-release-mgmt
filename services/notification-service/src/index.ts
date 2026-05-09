import { loadConfig } from "./config.js";
import { setupHttp, buildIngressClient } from "./http.js";
import { setupKafka } from "./kafka.js";
import { setupRestate, configureKafkaSend } from "./restate.js";

const config = loadConfig();

const ingressClient = buildIngressClient(config.RESTATE_INGRESS_URL);
const app = setupHttp({ ingressClient });

app.listen(config.HTTP_PORT, () => {
  console.log(`notification-service HTTP listening on ${config.HTTP_PORT}`);
});

const kafka = await setupKafka({
  brokers: config.KAFKA_BOOTSTRAP_SERVERS,
  consumersEnabled: config.KAFKA_CONSUMERS_ENABLED,
});

configureKafkaSend(kafka.send);

await setupRestate({
  registerHandlers: config.RESTATE_REGISTER_HANDLERS,
  port: config.RESTATE_HANDLER_PORT,
});
