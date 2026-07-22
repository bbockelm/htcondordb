import { test, expect } from '@grafana/plugin-e2e';

// Capstone: a continuous aggregate -- CREATE MATERIALIZED VIEW ... GROUP BY time_bucket(...)
// -- graphs as a time series in Grafana. The view seals aged buckets to a per-view archive
// and evicts them from memory; the read unions the live backing with the sealed archive, so
// `SELECT ... FROM jobs_ca` returns the full series regardless of what has sealed. This
// dashboard reads the continuous aggregate (jobs_ca, created in sample-data.sql) over the
// sample jobs' QDate, with the panel range pinned over that data.
test('a continuous aggregate graphs as a time series', async ({ gotoDashboardPage, page }) => {
  await gotoDashboardPage({ uid: 'htcondordb-ca-e2e' });

  const content = page.getByTestId('data-testid panel content');
  await expect(content).toContainText('metric_events', { timeout: 20000 });
  await expect(content).not.toContainText('No data');
});
