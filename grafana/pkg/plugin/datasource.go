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
	_ instancemgmt.InstanceDisposer = (*Datasource)(nil)
)

// datasourceSettings is the non-secret JSONData saved by the ConfigEditor.
type datasourceSettings struct {
	// Address is the htcondordb server (HTCondor sinful string or host:port).
	Address string `json:"address"`
	// ConnectTimeoutSeconds bounds dialing + the CEDAR handshake (0 = default).
	ConnectTimeoutSeconds int `json:"connectTimeoutSeconds"`
}

// Datasource is one configured htcondordb datasource instance. It holds no
// long-lived connection: each QueryData/CheckHealth dials its own dbrpc session,
// which keeps the plugin robust to server restarts and avoids stale sessions.
type Datasource struct {
	cfg connConfig
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
	return &Datasource{cfg: cfg}, nil
}

// Dispose is called when an instance is replaced or removed. Nothing to release --
// the plugin holds no persistent connections.
func (d *Datasource) Dispose() {}

// QueryData opens one dbrpc session for the whole request and runs each query
// through the repl SQL engine. A per-query error is attached to that query's
// response so one bad query does not sink the panel's other queries; a connection
// failure fails the whole batch (there is nothing to run against).
func (d *Datasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	resp := backend.NewQueryDataResponse()

	sess, err := connect(ctx, d.cfg)
	if err != nil {
		for _, q := range req.Queries {
			resp.Responses[q.RefID] = backend.ErrDataResponse(backend.StatusBadGateway, err.Error())
		}
		return resp, nil
	}
	defer sess.cleanup()

	exec := repl.NewExecutor(sess.client, repl.ExecConfig{})
	for _, q := range req.Queries {
		resp.Responses[q.RefID] = d.runQuery(exec, q)
	}
	return resp, nil
}

func (d *Datasource) runQuery(exec *repl.Executor, q backend.DataQuery) backend.DataResponse {
	var qm queryModel
	if err := json.Unmarshal(q.JSON, &qm); err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, fmt.Sprintf("parsing query: %v", err))
	}
	sql, err := qm.toSQL(newTimeRange(q.TimeRange.From, q.TimeRange.To))
	if err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, err.Error())
	}
	res, err := exec.ExecString(sql)
	if err != nil {
		msg := err.Error()
		if h := repl.HintFor(err); h != "" {
			msg += " (" + h + ")"
		}
		return backend.ErrDataResponse(backend.StatusInternal, msg)
	}
	frame := resultToFrame(q.RefID, res, qm.TimeField, qm.Format)
	if frame.Meta == nil {
		frame.Meta = &data.FrameMeta{}
	}
	frame.Meta.ExecutedQueryString = sql // shown in Grafana's "Query inspector"
	return backend.DataResponse{Frames: data.Frames{frame}}
}

// CheckHealth verifies the datasource can connect, authenticate, and run a query.
// The "Save & test" button in the ConfigEditor calls this.
func (d *Datasource) CheckHealth(ctx context.Context, _ *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	sess, err := connect(ctx, d.cfg)
	if err != nil {
		return &backend.CheckHealthResult{
			Status:  backend.HealthStatusError,
			Message: "Connection failed: " + err.Error(),
		}, nil
	}
	defer sess.cleanup()

	// A cheap probe query proves the dbrpc session works end to end. If it fails we
	// are still connected + authenticated (e.g. the default table may not exist),
	// so report OK with the detail rather than a hard error.
	exec := repl.NewExecutor(sess.client, repl.ExecConfig{})
	if _, err := exec.ExecString("SELECT COUNT(*) FROM " + dbrpc.DefaultTable); err != nil {
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
