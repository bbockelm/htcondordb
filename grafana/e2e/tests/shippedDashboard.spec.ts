import { test, expect } from '@grafana/plugin-e2e';

// The plugin ships an "HTCondorDB Overview" dashboard (src/dashboards, bundled via
// plugin.json includes). Provisioned here, it exercises template variables (the
// Owner query variable populated from the DB via metricFindQuery) and the builder/
// SQL panels. Open it and assert the variable resolved and the panels render.
test('shipped dashboard renders with a DB-populated Owner variable', async ({ gotoDashboardPage, page }) => {
  const dashboardPage = await gotoDashboardPage({ uid: 'htcondordb-overview' });

  // The Owner query variable is populated from the DB (SELECT DISTINCT Owner FROM
  // jobs -> metricFindQuery) and selects a real owner.
  await expect(page.getByTestId('data-testid Dashboard template variables submenu Label Owner')).toBeVisible();
  await expect(page.getByText(/^(alice|bob|carol)$/).first()).toBeVisible();

  // The curated panels render.
  await expect(dashboardPage.getPanelByTitle('Jobs by owner').locator).toBeVisible();
  await expect(dashboardPage.getPanelByTitle('Machines by state').locator).toBeVisible();
});
