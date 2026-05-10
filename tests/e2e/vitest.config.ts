import { defineConfig } from "vitest/config";

const includeScenarios = process.env.E2E_SCENARIOS === "1";

export default defineConfig({
  test: {
    include: ["**/*.test.ts"],
    exclude: [
      "**/node_modules/**",
      "**/dist/**",
      ...(includeScenarios ? [] : ["s*.test.ts", "k*.test.ts"]),
    ],
    environment: "node",
    globals: false,
    testTimeout: 5 * 60_000,
    // 15 min hook timeout: a K-scenario beforeAll deploys 5 canaries
    // sequentially; under continuous pre-warm each canary takes ~90-120s
    // to satisfy its kafkaConsumer readiness gate, so 5 × 2min = 10min
    // worst case. Default 5min is too tight.
    hookTimeout: 15 * 60_000,
    pool: "forks",
    poolOptions: {
      forks: { singleFork: true },
    },
    fileParallelism: false,
    reporters: ["verbose"],
  },
});
