import { test, expect } from '@grafana/plugin-e2e';

// The `jobs_by_owner` materialized view is exported on htcondordb's /metrics as the
// gauge jobs_by_owner_jobs (one series per owner) and scraped by Prometheus. This
// builds a Grafana time-series panel on the provisioned Prometheus datasource that
// graphs that metric, and asserts the per-owner series render.
test('a materialized view exported to Prometheus graphs as a time series', async ({ gotoDashboardPage, page }) => {
  const dashboardPage = await gotoDashboardPage({});
  const panelEditPage = await dashboardPage.addPanel();
  await panelEditPage.datasource.set('Prometheus');
  await panelEditPage.setVisualization('Time series');

  // Enter the view's exported gauge as a PromQL query (Code mode). insertText +
  // Escape avoids the autocomplete popup swallowing the query.
  await page.getByTestId('data-testid QueryEditorModeToggle').getByLabel('Code').click();
  await page.waitForTimeout(800);
  await page.getByTestId('data-testid prometheus query field').click();
  await page.keyboard.insertText('jobs_by_owner_jobs');
  await page.keyboard.press('Escape');
  await page.waitForTimeout(400);
  await page.getByTestId('data-testid RefreshPicker run button').click();

  // The time series renders one line per owner (the view's label column); the
  // panel content (legend) shows the metric with each owner series.
  const panelContent = page.getByTestId('data-testid panel content');
  await expect(panelContent).toContainText('jobs_by_owner_jobs', { timeout: 20000 });
  await expect(panelContent).toContainText('owner="alice"');
});
