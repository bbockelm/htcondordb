package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/instancemgmt"
	"github.com/grafana/grafana-plugin-sdk-go/data"

	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/repl"
)

// Compile-time checks that Datasource satisfies the SDK handler interfaces.
var (
	_ backend.QueryDataHandler      = (*Datasource)(nil)
	_ backend.CheckHealthHandler    = (*Datasource)(nil)
	_ backend.CallResourceHandler   = (*Datasource)(nil)
	_ backend.StreamHandler         = (*Datasource)(nil)
	_ instancemgmt.InstanceDisposer = (*Datasource)(nil)
)

// datasourceSettings is the non-secret JSONData saved by the ConfigEditor.
type datasourceSettings struct {
	// Address is the htcondordb server (HTCondor sinful string or host:port).
	Address string `json:"address"`
	// ConnectTimeoutSeconds bounds dialing + the CEDAR handshake (0 = default).
	ConnectTimeoutSeconds int `json:"connectTimeoutSeconds"`
}

// Datasource is one configured htcondordb datasource instance. Request/response
// queries share a pooled dbrpc session (connManager); streaming queries open their
// own watch connection.
type Datasource struct {
	cfg   connConfig
	uid   string
	conns *connManager
}

// NewDatasource is the instance factory registered with the SDK. Grafana calls it
// once per datasource configuration (and again whenever the config changes).
func NewDatasource(_ context.Context, s backend.DataSourceInstanceSettings) (instancemgmt.Instance, error) {
	var js datasourceSettings
	if len(s.JSONData) > 0 {
		if err := json.Unmarshal(s.JSONData, &js); err != nil {
			return nil, fmt.Errorf("parsing datasource settings: %w", err)
		}
	}
	addr := strings.TrimSpace(js.Address)
	if addr == "" {
		addr = strings.TrimSpace(s.URL) // fall back to the standard URL field
	}
	if addr == "" {
		return nil, errors.New("htcondordb address is not configured")
	}
	cfg := connConfig{
		Address: addr,
		Token:   s.DecryptedSecureJSONData["token"], // empty -> anonymous read-only
	}
	if js.ConnectTimeoutSeconds > 0 {
		cfg.ConnectTimeout = time.Duration(js.ConnectTimeoutSeconds) * time.Second
	}
	return &Datasource{cfg: cfg, uid: s.UID, conns: newConnManager(cfg)}, nil
}

// Dispose is called when an instance is replaced or removed; close the pooled
// session.
func (d *Datasource) Dispose() { d.conns.close() }

// QueryData runs each query. Regular queries share one pooled dbrpc session and
// execute through the repl SQL engine; streaming queries return a live channel
// (the StreamHandler drives it). A per-query error is attached to that query's
// response so one bad query does not sink the others.
func (d *Datasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	resp := backend.NewQueryDataResponse()

	var exec *repl.Executor
	var execErr error
	getExec := func() (*repl.Executor, error) {
		if exec == nil && execErr == nil {
			exec, execErr = d.conns.executor(ctx)
		}
		return exec, execErr
	}

	connBroken := false
	for _, q := range req.Queries {
		var qm queryModel
		if err := json.Unmarshal(q.JSON, &qm); err != nil {
			resp.Responses[q.RefID] = backend.ErrDataResponse(backend.StatusBadRequest, fmt.Sprintf("parsing query: %v", err))
			continue
		}
		if qm.Stream {
			resp.Responses[q.RefID] = d.streamQueryResponse(q.RefID, qm)
			continue
		}
		e, err := getExec()
		if err != nil {
			resp.Responses[q.RefID] = backend.ErrDataResponse(backend.StatusBadGateway, err.Error())
			continue
		}
		dr, broken := d.runSQL(e, q, qm)
		connBroken = connBroken || broken
		resp.Responses[q.RefID] = dr
	}
	// If the shared session died mid-batch, drop it so the next request redials.
	if connBroken {
		d.conns.reset()
	}
	return resp, nil
}

// runSQL renders the query to SQL, executes it, and maps the result to a frame.
// The bool reports whether the failure was connection-level (so the caller resets
// the pooled session).
func (d *Datasource) runSQL(exec *repl.Executor, q backend.DataQuery, qm queryModel) (backend.DataResponse, bool) {
	sql, err := qm.toSQL(newTimeRange(q.TimeRange.From, q.TimeRange.To))
	if err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, err.Error()), false
	}
	res, err := exec.ExecString(sql)
	if err != nil {
		msg := err.Error()
		if h := repl.HintFor(err); h != "" {
			msg += " (" + h + ")"
		}
		status := backend.StatusInternal
		if isConnError(err) {
			status = backend.StatusBadGateway
		}
		return backend.ErrDataResponse(status, msg), isConnError(err)
	}
	frame := resultToFrame(q.RefID, res, qm.TimeField, qm.Format)
	if frame.Meta == nil {
		frame.Meta = &data.FrameMeta{}
	}
	frame.Meta.ExecutedQueryString = sql // shown in Grafana's "Query inspector"
	return backend.DataResponse{Frames: data.Frames{frame}}, false
}

// CheckHealth verifies the datasource can connect, authenticate, and run a query.
func (d *Datasource) CheckHealth(ctx context.Context, _ *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	exec, err := d.conns.executor(ctx)
	if err != nil {
		d.conns.reset()
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: "Connection failed: " + err.Error(),
		}, nil
	}
	if _, err := exec.ExecString("SELECT COUNT(*) FROM " + dbrpc.DefaultTable); err != nil {
		if isConnError(err) {
			d.conns.reset()
			return &backend.CheckHealthResult{Status: backend.HealthStatusError, Message: "Connection failed: " + err.Error()}, nil
		}
		// Connected + authenticated, but the probe query failed (e.g. the default
		// table does not exist) -- still healthy.
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusOk,
			Message: "Connected to htcondordb (probe query failed: " + err.Error() + ")",
		}, nil
	}
	return &backend.CheckHealthResult{
		Status:  backend.HealthStatusOk,
		Message: "Connected to htcondordb",
	}, nil
}
