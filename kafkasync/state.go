package kafkasync

import "encoding/json"

// State is a Kafka exporter's durable resume state, stored as an opaque blob in the catalog
// via dbrpc PutExporterState. It is checkpointed only AFTER the corresponding records have
// been acknowledged by the broker, which is what makes delivery at-least-once (a crash
// before the checkpoint re-delivers the tail on restart).
type State struct {
	// WireCursor is the watch cursor to resume from -- the last point whose changes were
	// acknowledged by the broker. Empty means "from the beginning" (a full replay).
	WireCursor []byte `json:"cursor,omitempty"`

	// ExportSeq is a strictly monotonic counter stamped into each produced record's
	// version header. It is decoupled from the watch cursor on purpose: the cursor's epoch
	// is randomized on every DB restart (so cursor sequences are not comparable across
	// restarts), whereas ExportSeq only ever increases here, giving consumers a version
	// they can compare to keep the latest write per key even across a full re-materialize.
	ExportSeq uint64 `json:"exportSeq"`

	// KeyVersions records the ExportSeq last produced for each live key. It exists to
	// detect deletes that the change stream cannot report: on a WatchReset the stream
	// replays only the current snapshot (no before-image), so keys deleted during the gap
	// simply never reappear. After a reset we diff the fresh snapshot against this map and
	// emit a tombstone for every key that vanished. Memory is O(live keys).
	KeyVersions map[string]uint64 `json:"keyVersions,omitempty"`
}

// newState returns an empty state (fresh exporter: replay from the beginning).
func newState() *State {
	return &State{KeyVersions: map[string]uint64{}}
}

// decodeState parses a checkpoint blob; a nil/empty blob yields a fresh state.
func decodeState(blob []byte) (*State, error) {
	if len(blob) == 0 {
		return newState(), nil
	}
	var s State
	if err := json.Unmarshal(blob, &s); err != nil {
		return nil, err
	}
	if s.KeyVersions == nil {
		s.KeyVersions = map[string]uint64{}
	}
	return &s, nil
}

// encode serializes the state for PutExporterState.
func (s *State) encode() ([]byte, error) {
	return json.Marshal(s)
}
