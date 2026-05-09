import { loadConfig } from "./config.js";
import { setupHttp, buildIngressClient } from "./http.js";

const config = loadConfig();

const ingressClient = buildIngressClient(config.RESTATE_INGRESS_URL);
const app = setupHttp({ ingressClient });

app.listen(config.HTTP_PORT, () => {
  console.log(`notification-service HTTP listening on ${config.HTTP_PORT}`);
});

// Kafka + Restate setup wired in Tasks 18 + 19.
