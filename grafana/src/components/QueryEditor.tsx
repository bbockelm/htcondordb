import React, { ChangeEvent, useEffect, useState } from 'react';

import { QueryEditorProps, SelectableValue } from '@grafana/data';
import {
  Button,
  CodeEditor,
  IconButton,
  InlineField,
  InlineFieldRow,
  Input,
  MultiSelect,
  RadioButtonGroup,
  Select,
} from '@grafana/ui';

import { DataSource } from '../datasource';
import {
  AGG_FUNCS,
  DEFAULT_QUERY,
  EditorMode,
  FILTER_OPS,
  FilterDef,
  HtcondordbDataSourceOptions,
  HtcondordbQuery,
  MetricDef,
} from '../types';

type Props = QueryEditorProps<DataSource, HtcondordbQuery, HtcondordbDataSourceOptions>;

const LABEL_WIDTH = 14;

const MODE_OPTIONS: Array<SelectableValue<EditorMode>> = [
  { label: 'Builder', value: 'builder' },
  { label: 'SQL', value: 'code' },
];
const SOURCE_OPTIONS: Array<SelectableValue<boolean>> = [
  { label: 'Query', value: false },
  { label: 'Live (WATCH)', value: true },
];
const FORMAT_OPTIONS: Array<SelectableValue<'table' | 'timeseries'>> = [
  { label: 'Table', value: 'table' },
  { label: 'Time series', value: 'timeseries' },
];

const opt = (s: string): SelectableValue<string> => ({ label: s, value: s });

export function QueryEditor({ datasource, query, onChange, onRunQuery }: Props) {
  const q = { ...DEFAULT_QUERY, ...query };
  const mode: EditorMode = q.editorMode ?? 'builder';

  const [tables, setTables] = useState<string[]>([]);
  const [attrs, setAttrs] = useState<string[]>([]);

  useEffect(() => {
    let live = true;
    datasource.getTables().then((t) => live && setTables(t));
    return () => {
      live = false;
    };
  }, [datasource]);

  useEffect(() => {
    let live = true;
    if (q.table) {
      datasource.getAttributes(q.table).then((a) => live && setAttrs(a));
    } else {
      setAttrs([]);
    }
    return () => {
      live = false;
    };
  }, [datasource, q.table]);

  const tableOptions = tables.map(opt);
  const attrOptions = attrs.map(opt);

  const set = (patch: Partial<HtcondordbQuery>) => onChange({ ...q, ...patch });
  const setAndRun = (patch: Partial<HtcondordbQuery>) => {
    onChange({ ...q, ...patch });
    onRunQuery();
  };

  const metrics = q.metrics ?? [];
  const filters = q.filters ?? [];
  const updateMetric = (i: number, patch: Partial<MetricDef>) =>
    set({ metrics: metrics.map((m, j) => (j === i ? { ...m, ...patch } : m)) });
  const updateFilter = (i: number, patch: Partial<FilterDef>) =>
    set({ filters: filters.map((f, j) => (j === i ? { ...f, ...patch } : f)) });

  const tableSelect = (
    <InlineField label="Table" labelWidth={LABEL_WIDTH} tooltip="Discovered from the server; type to add a custom name.">
      <Select
        width={28}
        data-testid="htcondordb-query-table"
        options={tableOptions}
        value={q.table ? opt(q.table) : null}
        allowCustomValue
        placeholder="jobs"
        onChange={(v) => setAndRun({ table: v?.value ?? '' })}
      />
    </InlineField>
  );

  return (
    <div>
      <InlineFieldRow>
        <InlineField label="Editor" labelWidth={LABEL_WIDTH}>
          <RadioButtonGroup options={MODE_OPTIONS} value={mode} onChange={(v) => set({ editorMode: v })} />
        </InlineField>
        {mode === 'builder' && (
          <InlineField label="Source" labelWidth={LABEL_WIDTH} tooltip="Query runs once; Live tails the table's change stream (WATCH).">
            <RadioButtonGroup options={SOURCE_OPTIONS} value={q.stream ?? false} onChange={(v) => setAndRun({ stream: v })} />
          </InlineField>
        )}
        {mode === 'builder' && !q.stream && (
          <InlineField label="Format" labelWidth={LABEL_WIDTH}>
            <RadioButtonGroup options={FORMAT_OPTIONS} value={q.format ?? 'table'} onChange={(v) => setAndRun({ format: v })} />
          </InlineField>
        )}
      </InlineFieldRow>

      {mode === 'code' && (
        <CodeEditor
          language="sql"
          height={180}
          value={q.rawSql ?? ''}
          showMiniMap={false}
          showLineNumbers
          onBlur={(value) => setAndRun({ rawSql: value })}
        />
      )}

      {mode === 'builder' && q.stream && (
        <>
          <InlineFieldRow>{tableSelect}</InlineFieldRow>
          <InlineFieldRow>
            <InlineField label="Columns" labelWidth={LABEL_WIDTH} tooltip="Attributes to include in each streamed change (besides key + kind).">
              <MultiSelect
                width={48}
                options={attrOptions}
                value={(q.columns ?? []).map(opt)}
                allowCustomValue
                placeholder="Owner, JobStatus"
                onChange={(vs) => setAndRun({ columns: vs.map((v) => v.value!).filter(Boolean) })}
              />
            </InlineField>
          </InlineFieldRow>
        </>
      )}

      {mode === 'builder' && !q.stream && (
        <>
          <InlineFieldRow>
            {tableSelect}
            <InlineField label="Time field" labelWidth={LABEL_WIDTH} tooltip="Attribute constrained to the dashboard time range (unix epoch), e.g. QDate.">
              <Select
                width={24}
                options={attrOptions}
                value={q.timeField ? opt(q.timeField) : null}
                allowCustomValue
                isClearable
                placeholder="QDate"
                onChange={(v) => setAndRun({ timeField: v?.value ?? '' })}
              />
            </InlineField>
          </InlineFieldRow>

          <InlineFieldRow>
            <InlineField label="Columns" labelWidth={LABEL_WIDTH} tooltip="Plain attributes to select (omit when using only aggregates).">
              <MultiSelect
                width={48}
                options={attrOptions}
                value={(q.columns ?? []).map(opt)}
                allowCustomValue
                placeholder="Owner, JobStatus"
                onChange={(vs) => setAndRun({ columns: vs.map((v) => v.value!).filter(Boolean) })}
              />
            </InlineField>
          </InlineFieldRow>

          <InlineFieldRow>
            <InlineField label="Group by" labelWidth={LABEL_WIDTH}>
              <MultiSelect
                width={48}
                options={attrOptions}
                value={(q.groupBy ?? []).map(opt)}
                allowCustomValue
                placeholder="Owner"
                onChange={(vs) => setAndRun({ groupBy: vs.map((v) => v.value!).filter(Boolean) })}
              />
            </InlineField>
          </InlineFieldRow>

          {metrics.map((m, i) => (
            <InlineFieldRow key={`m-${i}`}>
              <InlineField label={i === 0 ? 'Metric' : ' '} labelWidth={LABEL_WIDTH}>
                <Select
                  width={16}
                  options={AGG_FUNCS.map((f) => ({ label: f || '(plain)', value: f }))}
                  value={opt(m.func)}
                  onChange={(v) => updateMetric(i, { func: v.value ?? '' })}
                />
              </InlineField>
              <InlineField label="of" labelWidth={4}>
                <Select
                  width={22}
                  options={[opt('*'), ...attrOptions]}
                  value={m.attr ? opt(m.attr) : null}
                  allowCustomValue
                  placeholder="*"
                  onChange={(v) => setAndRun({ metrics: metrics.map((mm, j) => (j === i ? { ...mm, attr: v?.value ?? '' } : mm)) })}
                />
              </InlineField>
              <IconButton name="trash-alt" aria-label="remove metric" onClick={() => setAndRun({ metrics: metrics.filter((_, j) => j !== i) })} />
            </InlineFieldRow>
          ))}
          <Button variant="secondary" size="sm" icon="plus" onClick={() => set({ metrics: [...metrics, { func: 'COUNT', attr: '*' }] })}>
            Add metric
          </Button>

          {filters.map((f, i) => (
            <InlineFieldRow key={`f-${i}`}>
              <InlineField label={i === 0 ? 'Where' : ' '} labelWidth={LABEL_WIDTH}>
                <Select
                  width={20}
                  options={attrOptions}
                  value={f.attr ? opt(f.attr) : null}
                  allowCustomValue
                  placeholder="State"
                  onChange={(v) => updateFilter(i, { attr: v?.value ?? '' })}
                />
              </InlineField>
              <InlineField label=" " labelWidth={2}>
                <Select
                  width={10}
                  options={FILTER_OPS.map(opt)}
                  value={opt(f.op || '==')}
                  onChange={(v) => updateFilter(i, { op: v.value ?? '==' })}
                />
              </InlineField>
              <InlineField label=" " labelWidth={2}>
                <Input
                  width={20}
                  value={f.value}
                  placeholder="Unclaimed"
                  onChange={(e: ChangeEvent<HTMLInputElement>) => updateFilter(i, { value: e.currentTarget.value })}
                  onBlur={() => onRunQuery()}
                />
              </InlineField>
              <IconButton name="trash-alt" aria-label="remove filter" onClick={() => setAndRun({ filters: filters.filter((_, j) => j !== i) })} />
            </InlineFieldRow>
          ))}
          <Button variant="secondary" size="sm" icon="plus" onClick={() => set({ filters: [...filters, { attr: '', op: '==', value: '' }] })}>
            Add filter
          </Button>

          <InlineFieldRow>
            <InlineField label="Order by" labelWidth={LABEL_WIDTH}>
              <Input
                width={24}
                value={q.orderBy ?? ''}
                placeholder="COUNT(*)"
                onChange={(e: ChangeEvent<HTMLInputElement>) => set({ orderBy: e.currentTarget.value })}
                onBlur={() => onRunQuery()}
              />
            </InlineField>
            <InlineField label="Dir" labelWidth={6}>
              <RadioButtonGroup
                options={[
                  { label: 'Asc', value: false },
                  { label: 'Desc', value: true },
                ]}
                value={q.orderDesc ?? false}
                onChange={(v) => setAndRun({ orderDesc: v })}
              />
            </InlineField>
            <InlineField label="Limit" labelWidth={7}>
              <Input
                width={12}
                type="number"
                value={q.limit ?? ''}
                placeholder="100"
                onChange={(e: ChangeEvent<HTMLInputElement>) => {
                  const n = parseInt(e.currentTarget.value, 10);
                  set({ limit: isNaN(n) ? undefined : n });
                }}
                onBlur={() => onRunQuery()}
              />
            </InlineField>
          </InlineFieldRow>
        </>
      )}
    </div>
  );
}
