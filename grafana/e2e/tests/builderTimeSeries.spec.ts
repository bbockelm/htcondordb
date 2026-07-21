import { test, expect } from '@grafana/plugin-e2e';

// The builder (no raw SQL) can produce a time series: set Format = Time series and a
// Time field, and the backend buckets that field by the panel interval
// (time_bucket). Drive the builder UI over the sample jobs' QDate and assert the
// panel graphs. The time range is pinned (UTC) over the sample data.
test('builder buckets a time field into a time series', async ({
  readProvisionedDataSource,
  panelEditPage,
  page,
}) => {
  const ds = await readProvisionedDataSource({ fileName: 'htcondordb.yaml' });
  await panelEditPage.datasource.set(ds.name);
  await panelEditPage.setVisualization('Time series');
  // Wide window around the sample data (2025-07-08) so any runner timezone still
  // contains it -- avoids the absolute-range/TZ fragility of a tight window.
  await panelEditPage.timeRange.set({ from: '2025-07-07 00:00:00', to: '2025-07-10 00:00:00' });

  const row = panelEditPage.getQueryEditorRow('A');

  // Format = Time series (scoped to the query row, not the visualization picker).
  await row.getByLabel('Time series').click();

  // Table = jobs.
  await row.getByTestId('htcondordb-query-table').click();
  await page.keyboard.type('jobs');
  await page.keyboard.press('Enter');

  // Time field = QDate (bucketed into the series axis).
  await row.getByTestId('htcondordb-query-timefield').click();
  await page.keyboard.type('QDate');
  await page.keyboard.press('Enter');

  // Add a COUNT(*) metric (the default when adding).
  await row.getByTestId('htcondordb-add-metric').click();

  await panelEditPage.refreshPanel();

  const content = page.getByTestId('data-testid panel content');
  await expect(content).toContainText('COUNT(*)', { timeout: 20000 });
  await expect(content).not.toContainText('No data');
});
