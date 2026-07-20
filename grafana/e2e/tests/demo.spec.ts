import { test, expect } from '@grafana/plugin-e2e';

import { installMouseHelper } from './mouse-helper';

// Render a visible cursor in the recording (Playwright videos omit the OS cursor).
// Init scripts persist across navigations, so installing before the body covers
// every page the demo visits.
test.beforeEach(async ({ page }) => {
  await installMouseHelper(page);
});

// A recorded walkthrough ("movie"): install/configure the htcondordb datasource,
// then use it to build a dashboard panel. Run via playwright.demo.config.ts, which
// records video and slows the interactions down so the result is watchable.
test('install and use the htcondordb datasource', async ({
  createDataSourceConfigPage,
  gotoDashboardPage,
  page,
}) => {
  const pause = (ms = 1200) => page.waitForTimeout(ms);

  await test.step('configure the datasource', async () => {
    const configPage = await createDataSourceConfigPage({
      type: 'bbockelm-htcondordb-datasource',
      name: 'HTCondorDB',
    });
    await pause();
    await page.getByTestId('htcondordb-config-address').fill('htcondordb:9630');
    await pause();
    await expect(configPage.saveAndTest()).toBeOK();
    await expect(configPage).toHaveAlert('success', { hasText: /Connected to htcondordb/ });
    await pause(1800);
  });

  await test.step('build a dashboard panel', async () => {
    const dashboardPage = await gotoDashboardPage({});
    await pause();
    const panelEditPage = await dashboardPage.addPanel();
    await panelEditPage.datasource.set('HTCondorDB');
    await pause();

    const row = panelEditPage.getQueryEditorRow('A');

    // Table: jobs
    await row.getByTestId('htcondordb-query-table').click();
    await page.keyboard.type('jobs');
    await page.keyboard.press('Enter');
    await pause();

    // Group by Owner
    await row.getByTestId('htcondordb-query-groupby').click();
    await page.keyboard.type('Owner');
    await page.keyboard.press('Enter');
    await pause();

    // Add a COUNT(*) metric -> SELECT Owner, COUNT(*) FROM jobs GROUP BY Owner
    await row.getByTestId('htcondordb-add-metric').click();
    await pause();

    await panelEditPage.setVisualization('Bar chart');
    await panelEditPage.refreshPanel();
    await pause(2500);
  });
});
