import React, { ChangeEvent } from 'react';

import { QueryEditorProps, SelectableValue } from '@grafana/data';
import {
  Button,
  CodeEditor,
  IconButton,
  InlineField,
  InlineFieldRow,
  Input,
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

const FORMAT_OPTIONS: Array<SelectableValue<'table' | 'timeseries'>> = [
  { label: 'Table', value: 'table' },
  { label: 'Time series', value: 'timeseries' },
];

const toList = (s: string): string[] =>
  s
    .split(',')
    .map((x) => x.trim())
    .filter((x) => x.length > 0);

export function QueryEditor({ query, onChange, onRunQuery }: Props) {
  const q = { ...DEFAULT_QUERY, ...query };
  const mode: EditorMode = q.editorMode ?? 'builder';

  const set = (patch: Partial<HtcondordbQuery>) => onChange({ ...q, ...patch });
  const setAndRun = (patch: Partial<HtcondordbQuery>) => {
    onChange({ ...q, ...patch });
    onRunQuery();
  };

  const metrics = q.metrics ?? [];
  const filters = q.filters ?? [];

  const updateMetric = (i: number, patch: Partial<MetricDef>) => {
    const next = metrics.slice();
    next[i] = { ...next[i], ...patch };
    set({ metrics: next });
  };
  const updateFilter = (i: number, patch: Partial<FilterDef>) => {
    const next = filters.slice();
    next[i] = { ...next[i], ...patch };
    set({ filters: next });
  };

  return (
    <div>
      <InlineFieldRow>
        <InlineField label="Editor" labelWidth={LABEL_WIDTH}>
          <RadioButtonGroup
            options={MODE_OPTIONS}
            value={mode}
            onChange={(v) => set({ editorMode: v })}
          />
        </InlineField>
        <InlineField label="Format" labelWidth={LABEL_WIDTH}>
          <RadioButtonGroup
            options={FORMAT_OPTIONS}
            value={q.format ?? 'table'}
            onChange={(v) => setAndRun({ format: v })}
          />
        </InlineField>
      </InlineFieldRow>

      {mode === 'code' ? (
        <CodeEditor
          language="sql"
          height={180}
          value={q.rawSql ?? ''}
          showMiniMap={false}
          showLineNumbers
          onBlur={(value) => setAndRun({ rawSql: value })}
        />
      ) : (
        <>
          <InlineFieldRow>
            <InlineField label="Table" labelWidth={LABEL_WIDTH} tooltip="e.g. jobs, machines, history">
              <Input
                width={24}
                value={q.table ?? ''}
                placeholder="jobs"
                onChange={(e: ChangeEvent<HTMLInputElement>) => set({ table: e.currentTarget.value })}
                onBlur={() => onRunQuery()}
              />
            </InlineField>
            <InlineField label="Time field" labelWidth={LABEL_WIDTH} tooltip="Attribute constrained to the dashboard time range (unix epoch), e.g. QDate.">
              <Input
                width={24}
                value={q.timeField ?? ''}
                placeholder="QDate"
                onChange={(e: ChangeEvent<HTMLInputElement>) => set({ timeField: e.currentTarget.value })}
                onBlur={() => onRunQuery()}
              />
            </InlineField>
          </InlineFieldRow>

          <InlineFieldRow>
            <InlineField label="Columns" labelWidth={LABEL_WIDTH} tooltip="Comma-separated plain attributes to select (ignored when using aggregates only).">
              <Input
                width={48}
                value={(q.columns ?? []).join(', ')}
                placeholder="Owner, JobStatus"
                onChange={(e: ChangeEvent<HTMLInputElement>) => set({ columns: toList(e.currentTarget.value) })}
                onBlur={() => onRunQuery()}
              />
            </InlineField>
          </InlineFieldRow>

          <InlineFieldRow>
            <InlineField label="Group by" labelWidth={LABEL_WIDTH} tooltip="Comma-separated group keys.">
              <Input
                width={48}
                value={(q.groupBy ?? []).join(', ')}
                placeholder="Owner"
                onChange={(e: ChangeEvent<HTMLInputElement>) => set({ groupBy: toList(e.currentTarget.value) })}
                onBlur={() => onRunQuery()}
              />
            </InlineField>
          </InlineFieldRow>

          {/* Metrics (aggregates) */}
          {metrics.map((m, i) => (
            <InlineFieldRow key={`m-${i}`}>
              <InlineField label={i === 0 ? 'Metric' : ' '} labelWidth={LABEL_WIDTH}>
                <Select
                  width={16}
                  options={AGG_FUNCS.map((f) => ({ label: f || '(plain)', value: f }))}
                  value={m.func}
                  onChange={(v) => updateMetric(i, { func: v.value ?? '' })}
                />
              </InlineField>
              <InlineField label="of" labelWidth={4}>
                <Input
                  width={20}
                  value={m.attr}
                  placeholder="* / Cpus"
                  onChange={(e: ChangeEvent<HTMLInputElement>) => updateMetric(i, { attr: e.currentTarget.value })}
                  onBlur={() => onRunQuery()}
                />
              </InlineField>
              <IconButton
                name="trash-alt"
                aria-label="remove metric"
                onClick={() => setAndRun({ metrics: metrics.filter((_, j) => j !== i) })}
              />
            </InlineFieldRow>
          ))}
          <Button
            variant="secondary"
            size="sm"
            icon="plus"
            onClick={() => set({ metrics: [...metrics, { func: 'COUNT', attr: '*' }] })}
          >
            Add metric
          </Button>

          {/* Filters (WHERE terms) */}
          {filters.map((f, i) => (
            <InlineFieldRow key={`f-${i}`}>
              <InlineField label={i === 0 ? 'Where' : ' '} labelWidth={LABEL_WIDTH}>
                <Input
                  width={20}
                  value={f.attr}
                  placeholder="State"
                  onChange={(e: ChangeEvent<HTMLInputElement>) => updateFilter(i, { attr: e.currentTarget.value })}
                  onBlur={() => onRunQuery()}
                />
              </InlineField>
              <InlineField label=" " labelWidth={2}>
                <Select
                  width={10}
                  options={FILTER_OPS.map((op) => ({ label: op, value: op }))}
                  value={f.op || '=='}
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
              <IconButton
                name="trash-alt"
                aria-label="remove filter"
                onClick={() => setAndRun({ filters: filters.filter((_, j) => j !== i) })}
              />
            </InlineFieldRow>
          ))}
          <Button
            variant="secondary"
            size="sm"
            icon="plus"
            onClick={() => set({ filters: [...filters, { attr: '', op: '==', value: '' }] })}
          >
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
            <InlineField label="Desc" labelWidth={8}>
              <RadioButtonGroup
                options={[
                  { label: 'Asc', value: false },
                  { label: 'Desc', value: true },
                ]}
                value={q.orderDesc ?? false}
                onChange={(v) => setAndRun({ orderDesc: v })}
              />
            </InlineField>
            <InlineField label="Limit" labelWidth={8}>
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
