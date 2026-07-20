import { test, expect } from '@grafana/plugin-e2e';

import { glideClick, installMouseHelper } from './mouse-helper';

// Render a visible cursor in the recording (Playwright videos omit the OS cursor).
// Init scripts persist across navigations, so installing before the body covers
// every page the demo visits.
test.beforeEach(async ({ page }) => {
  await installMouseHelper(page);
});

// A recorded walkthrough ("movie"): add + configure the htcondordb datasource,
// then use it to build a dashboard panel. Every interaction is a real, cursor-led
// click (the cursor glides to each target and arrives before clicking); see
// mouse-helper.ts. Run via playwright.demo.config.ts, which records video.
test('install and use the htcondordb datasource', async ({ gotoDashboardPage, page }) => {
  const pause = (ms = 900) => page.waitForTimeout(ms);

  await test.step('add and configure the datasource', async () => {
    // Grafana's "Add data source" list, then click the HTCondorDB type.
    await page.goto('/connections/datasources/new');
    await page.waitForLoadState('networkidle');
    await pause();
    await glideClick(page, page.locator('button', { hasText: 'HTCondorDB' }).first());
    await page.waitForURL(/datasources\/edit/);
    await pause();

    // Click into the address box, then type the server address.
    await glideClick(page, page.getByTestId('htcondordb-config-address'));
    await page.keyboard.type('htcondordb:9630', { delay: 55 });
    await pause();

    // Save & test -> the backend health check runs against htcondordb.
    await glideClick(page, page.getByRole('button', { name: /Save (&|and) test/i }));
    await expect(page.getByText(/Connected to htcondordb/i)).toBeVisible({ timeout: 15000 });
    await pause(1800);
  });

  await test.step('build a dashboard panel', async () => {
    const dashboardPage = await gotoDashboardPage({});
    await pause();
    const panelEditPage = await dashboardPage.addPanel();
    await panelEditPage.datasource.set('htcondordb');
    await pause();

    const row = panelEditPage.getQueryEditorRow('A');

    // Table: jobs
    await glideClick(page, row.getByTestId('htcondordb-query-table'));
    await page.keyboard.type('jobs', { delay: 55 });
    await page.keyboard.press('Enter');
    await pause();

    // Group by Owner
    await glideClick(page, row.getByTestId('htcondordb-query-groupby'));
    await page.keyboard.type('Owner', { delay: 55 });
    await page.keyboard.press('Enter');
    await pause();

    // Add a COUNT(*) metric -> SELECT Owner, COUNT(*) FROM jobs GROUP BY Owner
    await glideClick(page, row.getByTestId('htcondordb-add-metric'));
    await pause();

    await panelEditPage.setVisualization('Bar chart');
    await panelEditPage.refreshPanel();
    await pause(2500);
  });
});
