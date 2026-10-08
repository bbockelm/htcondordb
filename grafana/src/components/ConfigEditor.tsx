import React, { ChangeEvent } from 'react';

import { DataSourcePluginOptionsEditorProps } from '@grafana/data';
import { InlineField, Input, SecretInput } from '@grafana/ui';

import { HtcondordbDataSourceOptions, HtcondordbSecureJsonData } from '../types';

interface Props extends DataSourcePluginOptionsEditorProps<HtcondordbDataSourceOptions, HtcondordbSecureJsonData> {}

const LABEL_WIDTH = 22;

export function ConfigEditor(props: Props) {
  const { options, onOptionsChange } = props;
  const { jsonData, secureJsonFields, secureJsonData } = options;

  const onAddressChange = (e: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({ ...options, jsonData: { ...jsonData, address: e.target.value } });
  };

  const onPoolChange = (e: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({ ...options, jsonData: { ...jsonData, pool: e.target.value } });
  };

  const onNameChange = (e: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({ ...options, jsonData: { ...jsonData, name: e.target.value } });
  };

  const onTimeoutChange = (e: ChangeEvent<HTMLInputElement>) => {
    const n = parseInt(e.target.value, 10);
    onOptionsChange({
      ...options,
      jsonData: { ...jsonData, connectTimeoutSeconds: isNaN(n) ? undefined : n },
    });
  };

  const onTokenChange = (e: ChangeEvent<HTMLInputElement>) => {
    onOptionsChange({ ...options, secureJsonData: { ...secureJsonData, token: e.target.value } });
  };

  const onResetToken = () => {
    onOptionsChange({
      ...options,
      secureJsonFields: { ...secureJsonFields, token: false },
      secureJsonData: { ...secureJsonData, token: '' },
    });
  };

  return (
    <>
      <InlineField
        label="Pool"
        labelWidth={LABEL_WIDTH}
        tooltip="Collector to find the database through, as condor_status -pool takes it. Preferred over a fixed address: the daemon is looked up on every connection, so it is still found after a restart."
      >
        <Input
          width={40}
          data-testid="htcondordb-config-pool"
          value={jsonData.pool ?? ''}
          placeholder="cm-1.example.edu"
          onChange={onPoolChange}
        />
      </InlineField>

      <InlineField
        label="Database name"
        labelWidth={LABEL_WIDTH}
        tooltip="Which database in that pool, as condor_status -name takes it. Leave blank if the pool has only one; with several, the connection fails and lists them rather than picking one."
      >
        <Input
          width={40}
          data-testid="htcondordb-config-name"
          value={jsonData.name ?? ''}
          placeholder="htcondordb@ap40.example.edu"
          onChange={onNameChange}
        />
      </InlineField>

      <InlineField
        label="Address"
        labelWidth={LABEL_WIDTH}
        tooltip="Alternative to Pool: the server's address directly, as an HTCondor sinful string or host:port. Set this or Pool, not both."
      >
        <Input
          width={40}
          data-testid="htcondordb-config-address"
          value={jsonData.address ?? ''}
          placeholder="condordb.example.edu:9619"
          onChange={onAddressChange}
        />
      </InlineField>

      <InlineField
        label="Connect timeout (s)"
        labelWidth={LABEL_WIDTH}
        tooltip="Timeout for dialing and the CEDAR handshake. Blank = 30s."
      >
        <Input
          width={20}
          type="number"
          value={jsonData.connectTimeoutSeconds ?? ''}
          placeholder="30"
          onChange={onTimeoutChange}
        />
      </InlineField>

      <InlineField
        label="IDTOKEN"
        labelWidth={LABEL_WIDTH}
        tooltip="Optional HTCondor IDTOKEN. Leave blank for an anonymous, read-only connection."
      >
        <SecretInput
          width={40}
          isConfigured={Boolean(secureJsonFields?.token)}
          value={secureJsonData?.token ?? ''}
          placeholder="paste an IDTOKEN"
          onReset={onResetToken}
          onChange={onTokenChange}
        />
      </InlineField>
    </>
  );
}
