import { test, expect } from '@grafana/plugin-e2e';

// Build a dashboard panel against the provisioned htcondordb datasource: select it,
// point the builder at the `jobs` table, run it, and assert the sample data renders
// in a table panel (owners alice/bob/carol from the loaded ads).
test('builds a dashboard panel that queries htcondordb', async ({
  readProvisionedDataSource,
  panelEditPage,
  page,
}) => {
  const ds = await readProvisionedDataSource({ fileName: 'htcondordb.yaml' });
  await panelEditPage.datasource.set(ds.name);
  await panelEditPage.setVisualization('Table');

  // Builder: choose the `jobs` table (SELECT * by default).
  const row = panelEditPage.getQueryEditorRow('A');
  await row.getByTestId('htcondordb-query-table').click();
  await page.keyboard.type('jobs');
  await page.keyboard.press('Enter');

  await panelEditPage.refreshPanel();

  await expect(panelEditPage.panel.fieldNames).toContainText(['Owner']);
  await expect(panelEditPage.panel.data).toContainText(['alice']);
});
