package opensearchsync

import "encoding/json"

// State is an OpenSearch exporter's durable resume state, stored as an opaque blob in the
// catalog via dbrpc PutExporterState. It is checkpointed only after OpenSearch has durably
// acknowledged the corresponding documents (the oldest fully-acked contiguous prefix — see
// bulk.go), which is what makes delivery at-least-once: a crash before the checkpoint
// re-indexes the tail on restart, and external versioning makes the re-index idempotent.
type State struct {
	// WireCursor is the watch cursor to resume from — the last point whose documents were
	// acknowledged by OpenSearch. Empty means "from the beginning" (a full replay).
	WireCursor []byte `json:"cursor,omitempty"`

	// ExportSeq is a strictly monotonic counter stamped into each document's external
	// version. It is decoupled from the watch cursor on purpose: the cursor's epoch is
	// randomized on every DB restart (so cursor sequences are not comparable across
	// restarts), whereas ExportSeq only ever increases here — giving OpenSearch a version it
	// can use (version_type=external) to drop stale/duplicate re-indexes as no-op conflicts.
	//
	// Unlike the Kafka exporter, the append-only history archive holds immutable documents
	// (unique id = GlobalJobId#RecordTime) and emits no deletes, so no per-key version map or
	// delete-sweep is needed.
	ExportSeq uint64 `json:"exportSeq"`
}

// newState returns an empty state (fresh exporter: replay from the beginning).
func newState() *State { return &State{} }

// decodeState parses a checkpoint blob; a nil/empty blob yields a fresh state.
func decodeState(blob []byte) (*State, error) {
	if len(blob) == 0 {
		return newState(), nil
	}
	var s State
	if err := json.Unmarshal(blob, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// encode serializes the state for PutExporterState.
func (s *State) encode() ([]byte, error) { return json.Marshal(s) }
