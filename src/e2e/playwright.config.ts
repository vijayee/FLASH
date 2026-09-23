import { defineConfig } from '@playwright/test';

export default defineConfig({
  // Specs live directly in src/e2e/test/ (plan layout); testDir is relative
  // to this config file.
  testDir: 'test',
  timeout: 120_000,
  // Specs share one signaling server on one port (see startSignaling), so
  // parallel workers would collide on the port.
  workers: 1,
  use: {
    ignoreHTTPSErrors: true,
    // Failed-run forensics (Task 12 reads runs/): last frames + video for
    // the failing spec; one retry only for the slow geo runs.
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    video: 'retain-on-failure',
    launchOptions: {
      // Deterministic media for getUserMedia-driven specs, sandboxless for
      // containerized/netns peers, and autoplay allowed for the remote
      // <video> render (Task 4 checks audio too).
      args: [
        '--use-fake-ui-for-media-stream',
        '--use-fake-device-for-media-stream',
        '--autoplay-policy=no-user-gesture-required',
        '--no-sandbox',
      ],
    },
  },
  // One retry only for the slow geo runs.
  retries: process.env.E2E_GEO ? 1 : 0,
  reporter: [['list'], ['html', { open: 'never', outputFolder: 'runs/report' }]],
  // Opt-OUT tagging: every spec runs by default, so a title forgetting
  // `@local` still executes instead of silently vanishing from a green run.
  // @geo specs drive REAL Azure-region latency and are ignored unless the
  // orchestrator opts in via E2E_GEO. NOTE: testIgnore matches file PATHS,
  // not titles (verified: a /@geo/ title regex never excludes anything) —
  // so the exclusion is by spec file here, and geo.spec.ts additionally
  // carries a title-level test.skip guard for direct invocations.
  testIgnore: process.env.E2E_GEO ? [] : /geo\.spec\.ts/,
});