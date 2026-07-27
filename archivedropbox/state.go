package archivedropbox

import "encoding/json"

// State is a dropbox exporter's durable resume state, stored as an opaque blob in the catalog via
// dbrpc PutExporterState. It is persisted only AFTER the tarball it describes has been fsync'd and
// atomically renamed into the dropbox (and the directory fsync'd), which is what makes delivery
// at-least-once: a crash before the checkpoint re-exports the last batch (a duplicate tarball) but
// never loses it.
type State struct {
	// WireCursor is the watch cursor to resume from -- the batch boundary of the last tarball
	// durably placed in the dropbox. Empty means "from the oldest retained record" (a full replay).
	WireCursor []byte `json:"cursor,omitempty"`

	// TarballSeq is a strictly monotonic counter naming successive tarballs, so an operator (and
	// the consumer) sees a stable, ordered sequence across restarts.
	TarballSeq uint64 `json:"tarballSeq"`

	// LastRecordUnix is the record-time of the last record written to a tarball (best-effort;
	// see recordTime). It is the start of the estimated lost range if the cursor is later pruned.
	LastRecordUnix int64 `json:"lastRecordUnix,omitempty"`

	// Status is the exporter's live health/progress, refreshed on every roll/flush tick so the
	// htcondordb daemon can read it via LoadExporterState -- to detect a stalled exporter and to
	// surface progress. The daemon parses the same JSON shape without importing this module.
	Status Status `json:"status,omitempty"`
}

// Status is one exporter's live health/progress snapshot. Beat is the child's wall-clock at the
// last refresh; a beat that stops advancing means the exporter is wedged.
type Status struct {
	Beat        int64  `json:"beat"`        // unix seconds of the last refresh (liveness signal)
	DocsIndexed uint64 `json:"docsIndexed"` // cumulative records written into tarballs
	DocsSkipped uint64 `json:"docsSkipped"` // cumulative records dropped (undecodable ad)
	InFlight    int    `json:"inFlight"`    // records buffered for the next (not-yet-written) tarball
}

// newState returns an empty state (fresh exporter: replay from the oldest retained record).
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
