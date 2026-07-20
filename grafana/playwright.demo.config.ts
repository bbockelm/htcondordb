import { dirname, join } from 'node:path';
import { defineConfig, devices } from '@playwright/test';
import type { PluginOptions } from '@grafana/plugin-e2e';

// Config for the "movie": records a video of the full install-and-use walkthrough.
// Run on demand (npm run e2e:demo), separate from the CI suite. Slowed down and
// sized for a watchable recording.
const pluginE2eAuth = `${dirname(require.resolve('@grafana/plugin-e2e'))}/auth`;

export default defineConfig<PluginOptions>({
  testDir: './e2e/tests',
  fullyParallel: false,
  workers: 1,
  reporter: 'list',
  outputDir: './e2e/demo-output',
  use: {
    baseURL: process.env.GRAFANA_URL ?? 'http://localhost:3000',
    provisioningRootDir: join(process.cwd(), 'e2e/provisioning'),
    viewport: { width: 1280, height: 800 },
    video: { mode: 'on', size: { width: 1280, height: 800 } },
    // No slowMo: pacing comes from the cursor glides (mouse.move steps + per-step
    // waits) and explicit pauses; slowMo would multiply every glide step.
  },
  projects: [
    // No video for the login-setup step; only the demo walkthrough is recorded.
    { name: 'auth', testDir: pluginE2eAuth, testMatch: [/.*\.js/], use: { video: 'off' } },
    {
      name: 'demo',
      testMatch: /demo\.spec\.ts/,
      use: { ...devices['Desktop Chrome'], viewport: { width: 1280, height: 800 }, storageState: 'playwright/.auth/admin.json' },
      dependencies: ['auth'],
    },
  ],
});
