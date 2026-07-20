package plugin

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/repl"
)

// watchKindString names a dbrpc.WatchEvent.Kind (0 upsert, 1 delete, 2 reset).
func watchKindString(k uint8) string {
	switch k {
	case 0:
		return "upsert"
	case 1:
		return "delete"
	case 2:
		return "reset"
	default:
		return "unknown"
	}
}

// streamSpec is the streaming query encoded into a Grafana channel path: which
// table to watch and which attributes to project into each streamed row.
type streamSpec struct {
	Table   string   `json:"t"`
	Columns []string `json:"c,omitempty"`
}

// base64url(JSON) keeps the spec within Grafana's channel-path charset
// ([A-Za-z0-9_-]) without padding.
func encodeStreamPath(s streamSpec) string {
	b, _ := json.Marshal(s)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeStreamPath(p string) (streamSpec, error) {
	var s streamSpec
	b, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(p))
	if err != nil {
		return s, fmt.Errorf("bad stream path: %w", err)
	}
	if err := json.Unmarshal(b, &s); err != nil {
		return s, fmt.Errorf("bad stream path: %w", err)
	}
	if strings.TrimSpace(s.Table) == "" {
		return s, fmt.Errorf("stream path has no table")
	}
	return s, nil
}

// streamFrame builds a frame with the streaming schema -- time, key, kind, then one
// string field per projected column. With withRow=false it is the (0-row) schema
// frame used for subscription InitialData; with a row it is one change event.
func streamFrame(name string, spec streamSpec, row *streamRow) *data.Frame {
	tField := data.NewField("time", nil, []time.Time{})
	keyField := data.NewField("key", nil, []string{})
	kindField := data.NewField("kind", nil, []string{})
	colFields := make([]*data.Field, len(spec.Columns))
	for i, c := range spec.Columns {
		colFields[i] = data.NewField(c, nil, []string{})
	}
	if row != nil {
		tField.Append(row.t)
		keyField.Append(row.key)
		kindField.Append(row.kind)
		for i := range colFields {
			v := ""
			if i < len(row.cols) {
				v = row.cols[i]
			}
			colFields[i].Append(v)
		}
	}
	return data.NewFrame(name, append([]*data.Field{tField, keyField, kindField}, colFields...)...)
}

type streamRow struct {
	t    time.Time
	key  string
	kind string
	cols []string
}

// streamQueryResponse is the QueryData reply for a streaming query: an empty frame
// carrying the live channel Grafana subscribes to. The channel path encodes the
// watch spec so SubscribeStream/RunStream can reconstruct it.
func (d *Datasource) streamQueryResponse(refID string, qm queryModel) backend.DataResponse {
	table := strings.TrimSpace(qm.Table)
	if table == "" {
		return backend.ErrDataResponse(backend.StatusBadRequest, "streaming requires a table")
	}
	spec := streamSpec{Table: table, Columns: qm.Columns}
	frame := streamFrame(refID, spec, nil)
	frame.SetMeta(&data.FrameMeta{Channel: fmt.Sprintf("ds/%s/%s", d.uid, encodeStreamPath(spec))})
	return backend.DataResponse{Frames: data.Frames{frame}}
}

// SubscribeStream authorizes a subscription and returns the schema so the client
// buffers the right columns before data arrives.
func (d *Datasource) SubscribeStream(_ context.Context, req *backend.SubscribeStreamRequest) (*backend.SubscribeStreamResponse, error) {
	spec, err := decodeStreamPath(req.Path)
	if err != nil {
		return &backend.SubscribeStreamResponse{Status: backend.SubscribeStreamStatusNotFound}, nil
	}
	initial, err := backend.NewInitialFrame(streamFrame("", spec, nil), data.IncludeSchemaOnly)
	if err != nil {
		return nil, err
	}
	return &backend.SubscribeStreamResponse{
		Status:      backend.SubscribeStreamStatusOK,
		InitialData: initial,
	}, nil
}

// PublishStream denies client publishes -- this is a read-only tail.
func (d *Datasource) PublishStream(_ context.Context, _ *backend.PublishStreamRequest) (*backend.PublishStreamResponse, error) {
	return &backend.PublishStreamResponse{Status: backend.PublishStreamStatusPermissionDenied}, nil
}

// RunStream opens a dedicated dbrpc session, subscribes to the table's htcondordb
// WATCH stream from "now", and forwards each change to the subscriber as a frame
// until the client unsubscribes (ctx cancelled) or the server ends the stream.
func (d *Datasource) RunStream(ctx context.Context, req *backend.RunStreamRequest, sender *backend.StreamSender) error {
	spec, err := decodeStreamPath(req.Path)
	if err != nil {
		return err
	}
	// A watch holds its connection for the whole stream, so use a dedicated one
	// rather than the pooled request/response session.
	sess, err := connect(ctx, d.cfg)
	if err != nil {
		return err
	}
	defer sess.cleanup()

	exec := repl.NewExecutor(sess.client, repl.ExecConfig{})
	// Watch from "now": get the current head cursor and stream only changes committed
	// after it. A nil cursor instead replays the whole table (a reset + every
	// existing ad), which floods a live panel with current state; a live tail should
	// show changes as they happen. Fall back to nil (replay) only if the head is
	// unavailable.
	cursor, herr := exec.WatchHead(spec.Table)
	if herr != nil {
		cursor = nil
	}
	events, stop, err := exec.WatchStream(spec.Table, cursor)
	if err != nil {
		return fmt.Errorf("watch %s: %w", spec.Table, err)
	}
	defer stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-events:
			if !ok {
				return nil // server ended the stream / connection closed
			}
			frame := streamFrame("", spec, eventRow(spec, ev))
			if err := sender.SendFrame(frame, data.IncludeDataOnly); err != nil {
				return err
			}
		}
	}
}

// eventRow projects a watch event into a streamed row: the event time, the ad key,
// the change kind, and each requested attribute rendered as a string.
func eventRow(spec streamSpec, ev dbrpc.WatchEvent) *streamRow {
	row := &streamRow{t: time.Now().UTC(), key: ev.Key, kind: watchKindString(ev.Kind)}
	if len(spec.Columns) > 0 {
		row.cols = make([]string, len(spec.Columns))
		if ev.AdText != "" {
			if ad, err := classad.ParseOld(ev.AdText); err == nil {
				for i, c := range spec.Columns {
					row.cols[i] = ad.EvaluateAttr(c).String()
				}
			}
		}
	}
	return row
}
