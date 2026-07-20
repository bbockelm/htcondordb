import { test, expect } from '@grafana/plugin-e2e';

// Configure a fresh htcondordb datasource through the UI and confirm the backend
// health check (CheckHealth -> dbrpc -> htcondordb) succeeds.
test('configures the htcondordb datasource and passes the health check', async ({
  createDataSourceConfigPage,
  page,
}) => {
  const configPage = await createDataSourceConfigPage({ type: 'bbockelm-htcondordb-datasource' });
  await page.getByTestId('htcondordb-config-address').fill('htcondordb:9630');
  await expect(configPage.saveAndTest()).toBeOK();
  await expect(configPage).toHaveAlert('success', { hasText: /Connected to htcondordb/ });
});
