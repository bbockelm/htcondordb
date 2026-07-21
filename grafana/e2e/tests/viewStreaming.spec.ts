import { execSync } from 'node:child_process';

import { test, expect } from '@grafana/plugin-e2e';

// A materialized view is queryable like a table and maintained live, so the plugin
// can stream it (WATCH). Open a Live panel on the `jobs_by_owner` view, add a job
// for a brand-new owner, and assert the view's new group streams into the panel --
// i.e. the live materialized view updates as its base table changes.
test('streaming a live materialized view reflects base-table changes', async ({ gotoDashboardPage, page }) => {
  const dashboardPage = await gotoDashboardPage({});
  const panelEditPage = await dashboardPage.addPanel();
  await panelEditPage.datasource.set('htcondordb');
  await panelEditPage.setVisualization('Table');

  const row = panelEditPage.getQueryEditorRow('A');
  await row.getByLabel('Live (WATCH)', { exact: true }).click();
  await row.getByTestId('htcondordb-query-table').click();
  await page.keyboard.type('jobs_by_owner');
  await page.keyboard.press('Enter');

  await page.waitForTimeout(3000); // let the live channel subscribe

  // A job for a new owner -> the view maintains a new group for it.
  const owner = `live-${Date.now()}`;
  const sql = `INSERT INTO jobs (Key,Owner,JobStatus,RequestCpus) VALUES ('${owner}.0','${owner}',1,4)`;
  execSync(`docker compose -f e2e/docker-compose.e2e.yaml exec -T htcondordb htcondordb-cli -addr 127.0.0.1:9630 -e ${JSON.stringify(sql)}`, {
    stdio: 'pipe',
  });

  // The new group (keyed by owner) streams into the panel.
  await expect(panelEditPage.panel.data).toContainText([owner], { timeout: 20000 });
});
