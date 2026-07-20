import { dirname, join } from 'node:path';
import { defineConfig, devices } from '@playwright/test';
import type { PluginOptions } from '@grafana/plugin-e2e';

// @grafana/plugin-e2e ships an "auth" project that logs into Grafana and saves the
// session; the tests depend on it. Grafana + htcondordb come from
// e2e/docker-compose.e2e.yaml (GRAFANA_URL defaults to the mapped localhost:3000).
const pluginE2eAuth = `${dirname(require.resolve('@grafana/plugin-e2e'))}/auth`;

export default defineConfig<PluginOptions>({
  testDir: './e2e/tests',
  fullyParallel: false,
  workers: 1,
  reporter: process.env.CI ? [['list'], ['html', { open: 'never' }]] : 'list',
  use: {
    baseURL: process.env.GRAFANA_URL ?? 'http://localhost:3000',
    // Provisioned datasources for readProvisionedDataSource live here (also mounted
    // into the Grafana container by the compose file).
    provisioningRootDir: join(process.cwd(), 'e2e/provisioning'),
    trace: 'retain-on-failure',
  },
  projects: [
    { name: 'auth', testDir: pluginE2eAuth, testMatch: [/.*\.js/] },
    {
      name: 'e2e',
      // The demo (video walkthrough) is run on demand via playwright.demo.config.ts,
      // not as part of the normal/CI suite.
      testIgnore: /demo\.spec\.ts/,
      use: { ...devices['Desktop Chrome'], storageState: 'playwright/.auth/admin.json' },
      dependencies: ['auth'],
    },
  ],
});
