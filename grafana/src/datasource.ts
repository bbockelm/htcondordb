import { CoreApp, DataSourceInstanceSettings, ScopedVars } from '@grafana/data';
import { DataSourceWithBackend, getTemplateSrv } from '@grafana/runtime';

import { DEFAULT_QUERY, HtcondordbDataSourceOptions, HtcondordbQuery } from './types';

// DataSource is a thin frontend over the Go backend: DataSourceWithBackend sends
// each query's JSON to the plugin's QueryData over gRPC, so all SQL execution
// happens server-side. We only apply dashboard/template variables here.
export class DataSource extends DataSourceWithBackend<HtcondordbQuery, HtcondordbDataSourceOptions> {
  constructor(instanceSettings: DataSourceInstanceSettings<HtcondordbDataSourceOptions>) {
    super(instanceSettings);
  }

  getDefaultQuery(_app: CoreApp): Partial<HtcondordbQuery> {
    return DEFAULT_QUERY;
  }

  // Interpolate Grafana template variables into the raw SQL before it is sent to
  // the backend. ($__timeFilter and friends are expanded server-side.)
  applyTemplateVariables(query: HtcondordbQuery, scopedVars: ScopedVars): HtcondordbQuery {
    const srv = getTemplateSrv();
    return {
      ...query,
      rawSql: query.rawSql ? srv.replace(query.rawSql, scopedVars) : query.rawSql,
    };
  }

  // Builder discovery: the QueryEditor calls these to populate dropdowns from the
  // backend CallResource endpoints, so users don't memorize schema.
  async getTables(): Promise<string[]> {
    try {
      return (await this.getResource('tables')) ?? [];
    } catch {
      return [];
    }
  }

  async getAttributes(table: string): Promise<string[]> {
    if (!table) {
      return [];
    }
    try {
      return (await this.getResource('attributes', { table })) ?? [];
    } catch {
      return [];
    }
  }

  // Skip running queries that have nothing to execute (avoids spurious errors on
  // a freshly added, empty query row).
  filterQuery(query: HtcondordbQuery): boolean {
    if (query.hide) {
      return false;
    }
    if (query.editorMode === 'code') {
      return !!query.rawSql && query.rawSql.trim().length > 0;
    }
    return !!query.table && query.table.trim().length > 0;
  }
}
