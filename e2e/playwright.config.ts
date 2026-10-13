import { defineConfig } from "@playwright/test";
import { BASE_URL, STATE_FILE } from "./lib/env.ts";

export default defineConfig({
  testDir: "./specs",
  timeout: 60_000,
  expect: { timeout: 10_000 },
  fullyParallel: false,
  workers: 1,
  retries: 0,
  reporter: [["list"], ["json", { outputFile: "test-results/results.json" }]],
  globalSetup: "./global-setup.ts",
  globalTeardown: "./global-teardown.ts",
  use: {
    baseURL: BASE_URL,
    storageState: STATE_FILE,
    browserName: "chromium",
    viewport: { width: 1440, height: 900 },
    contextOptions: { reducedMotion: "reduce" },
    timezoneId: "UTC",
    locale: "en-US",
    trace: "retain-on-failure",
  },
});
