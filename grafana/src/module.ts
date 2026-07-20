import { DataSourcePlugin } from '@grafana/data';

import { ConfigEditor } from './components/ConfigEditor';
import { QueryEditor } from './components/QueryEditor';
import { DataSource } from './datasource';
import { HtcondordbDataSourceOptions, HtcondordbQuery } from './types';

export const plugin = new DataSourcePlugin<DataSource, HtcondordbQuery, HtcondordbDataSourceOptions>(DataSource)
  .setConfigEditor(ConfigEditor)
  .setQueryEditor(QueryEditor);
