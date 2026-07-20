import { execSync } from 'node:child_process';

import { test, expect } from '@grafana/plugin-e2e';

const COMPOSE = 'docker compose -f e2e/docker-compose.e2e.yaml';

// Run one SQL statement against the htcondordb container (mutating it mid-test so
// the WATCH stream fires an event).
function cli(sql: string): void {
  execSync(`${COMPOSE} exec -T htcondordb htcondordb-cli -addr 127.0.0.1:9630 -e ${JSON.stringify(sql)}`, {
    stdio: 'pipe',
  });
}

// Prove the streaming (WATCH) source works end to end: build a Live panel, then
// mutate htcondordb and assert the change streams into the panel live.
test('streaming (WATCH) updates appear live in a panel', async ({ gotoDashboardPage, page }) => {
  const dashboardPage = await gotoDashboardPage({});
  const panelEditPage = await dashboardPage.addPanel();
  await panelEditPage.datasource.set('htcondordb');
  await panelEditPage.setVisualization('Table');

  const row = panelEditPage.getQueryEditorRow('A');

  // Switch the source to Live (WATCH) and watch the machines table.
  await row.getByLabel('Live (WATCH)', { exact: true }).click();
  await row.getByTestId('htcondordb-query-table').click();
  await page.keyboard.type('machines');
  await page.keyboard.press('Enter');

  // Let the live channel subscribe (RunStream opens the WATCH from "now").
  await page.waitForTimeout(3000);

  // Mutate htcondordb -> a WATCH upsert event should stream into the panel.
  const key = `stream-${Date.now()}`;
  cli(`INSERT INTO machines (Key,Name,State,Cpus) VALUES ('${key}','${key}','Unclaimed',64)`);

  // The new row's key appears in the panel without re-running the query.
  await expect(panelEditPage.panel.data).toContainText([key], { timeout: 20000 });
});
