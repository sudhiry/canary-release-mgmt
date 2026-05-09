import { loadConfig } from "./config.js";
import { setupHttp, buildClient } from "./http.js";

const config = loadConfig();

const clients = {
  inventory: buildClient(config.INVENTORY_URL),
  payment: buildClient(config.PAYMENT_URL),
  notification: buildClient(config.NOTIFICATION_URL),
};

const app = setupHttp({ clients });

app.listen(config.HTTP_PORT, () => {
  console.log(`order-service HTTP listening on ${config.HTTP_PORT}`);
});

// Kafka + Restate setup are wired in Tasks 22 + 23.
