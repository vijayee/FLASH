import { defineConfig } from '@playwright/test';

export default defineConfig({
  // Specs live directly in src/e2e/test/ (plan layout); testDir is relative
  // to this config file.
  testDir: 'test',
  timeout: 120_000,
  use: {
    ignoreHTTPSErrors: true,
    launchOptions: {
      // Deterministic media for getUserMedia-driven specs, and sandboxless
      // for containerized/netns peers.
      args: [
        '--use-fake-ui-for-media-stream',
        '--use-fake-device-for-media-stream',
        '--no-sandbox',
      ],
    },
  },
  // Geo specs (tagged @geo) drive real Azure-region latency and only run
  // when the orchestrator opts in; everything else is @local.
  grep: process.env.E2E_GEO ? /@local|@geo/ : /@local/,
});