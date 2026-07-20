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
        label="Address"
        labelWidth={LABEL_WIDTH}
        tooltip="htcondordb server address: an HTCondor sinful string or host:port."
      >
        <Input
          width={40}
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
