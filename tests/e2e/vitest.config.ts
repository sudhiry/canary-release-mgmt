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
    hookTimeout: 5 * 60_000,
    pool: "forks",
    poolOptions: {
      forks: { singleFork: true },
    },
    fileParallelism: false,
    reporters: ["verbose"],
  },
});
