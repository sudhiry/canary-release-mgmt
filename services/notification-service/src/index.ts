import { loadConfig } from "./config.js";
import { setupHttp, buildIngressClient } from "./http.js";
import { setupRestate } from "./restate.js";

const config = loadConfig();

const ingressClient = buildIngressClient(config.RESTATE_INGRESS_URL);
const app = setupHttp({ ingressClient });

app.listen(config.HTTP_PORT, () => {
  console.log(`notification-service HTTP listening on ${config.HTTP_PORT}`);
});

await setupRestate({
  registerHandlers: config.RESTATE_REGISTER_HANDLERS,
  port: config.RESTATE_HANDLER_PORT,
});

// Kafka setup wired in Task 19.
