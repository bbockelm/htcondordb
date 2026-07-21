import { test, expect } from '@grafana/plugin-e2e';

// The `time_bucket(attr, 'width')` grouping function (repl) floors a unix-epoch
// attribute into fixed-width buckets, turning point-in-time rows into a time series.
// This provisioned dashboard runs it over the sample jobs' QDate in a Time series
// panel, with a time range pinned over the sample data; assert it graphs (a series
// named for the metric column, and not an empty "No data" panel).
test('time_bucket over QDate graphs as a time series', async ({ gotoDashboardPage, page }) => {
  await gotoDashboardPage({ uid: 'htcondordb-timeseries-e2e' });

  const content = page.getByTestId('data-testid panel content');
  await expect(content).toContainText('metric_jobs', { timeout: 20000 });
  await expect(content).not.toContainText('No data');
});
