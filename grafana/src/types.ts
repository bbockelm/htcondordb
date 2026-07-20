import { DataSourceJsonData } from '@grafana/data';
import { DataQuery } from '@grafana/schema';

export type EditorMode = 'builder' | 'code';

// MetricDef is one aggregate in the builder, e.g. {func:'AVG', attr:'Cpus'} or
// {func:'COUNT', attr:'*'}.
export interface MetricDef {
  func: string;
  attr: string;
}

// FilterDef is one WHERE term; the backend auto-quotes non-numeric values.
export interface FilterDef {
  attr: string;
  op: string;
  value: string;
}

// HtcondordbQuery mirrors the Go queryModel (pkg/plugin/query.go). The builder
// fields assemble SQL server-side; rawSql is used verbatim in code mode.
export interface HtcondordbQuery extends DataQuery {
  editorMode?: EditorMode;
  rawSql?: string;

  table?: string;
  columns?: string[];
  metrics?: MetricDef[];
  groupBy?: string[];
  filters?: FilterDef[];
  timeField?: string;
  orderBy?: string;
  orderDesc?: boolean;
  limit?: number;

  format?: 'table' | 'timeseries';

  // stream tails the table's change stream (htcondordb WATCH) live instead of
  // running a one-shot query. Builder-only; uses table + columns.
  stream?: boolean;
}

export const DEFAULT_QUERY: Partial<HtcondordbQuery> = {
  editorMode: 'builder',
  format: 'table',
};

export const AGG_FUNCS = ['', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX'];
export const FILTER_OPS = ['==', '!=', '>', '>=', '<', '<=', '=~', '!~'];

// Non-secret datasource config (JSONData).
export interface HtcondordbDataSourceOptions extends DataSourceJsonData {
  address?: string;
  connectTimeoutSeconds?: number;
}

// Secret datasource config (SecureJSONData).
export interface HtcondordbSecureJsonData {
  token?: string;
}
