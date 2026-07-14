package consistent

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/hashicorp/raft"
)

// FSM is the raft finite state machine: it applies committed mutation batches to
// a local ClassAd database. Every replica runs an identical FSM over identical
// log entries, so all converge on the same state.
type FSM struct {
	db *db.DB
}

// NewFSM builds an FSM over d. The FSM is d's sole writer in consistent mode.
func NewFSM(d *db.DB) *FSM { return &FSM{db: d} }

// Apply applies one committed log entry. The returned value becomes the
// ApplyFuture.Response on the leader (an error on failure, nil on success).
func (f *FSM) Apply(l *raft.Log) interface{} {
	if l.Type != raft.LogCommand {
		return nil
	}
	batch, err := DecodeBatch(l.Data)
	if err != nil {
		return err
	}
	return f.applyBatch(batch)
}

// applyBatch applies a batch in one transaction (all-or-nothing per replica).
func (f *FSM) applyBatch(b *Batch) error {
	tx := f.db.Begin()
	for _, op := range b.Ops {
		switch op.Kind {
		case OpNewClassAd:
			ad, err := classad.ParseOld(op.Value)
			if err != nil {
				tx.Abort()
				return fmt.Errorf("consistent: apply NewClassAd %s: %w", op.Key, err)
			}
			tx.NewClassAd(op.Key, ad)
		case OpDestroyClassAd:
			tx.DestroyClassAd(op.Key)
		case OpSetAttribute:
			if err := tx.SetAttribute(op.Key, op.Name, op.Value); err != nil {
				tx.Abort()
				return fmt.Errorf("consistent: apply SetAttribute %s.%s: %w", op.Key, op.Name, err)
			}
		case OpDeleteAttribute:
			tx.DeleteAttribute(op.Key, op.Name)
		default:
			tx.Abort()
			return fmt.Errorf("consistent: unknown op kind %d", op.Kind)
		}
	}
	return tx.Commit()
}

// snapshotRow is one ad captured in a snapshot.
type snapshotRow struct {
	Key    string `json:"key"`
	AdText string `json:"ad"` // new-ClassAd text, including private attributes
}

// Snapshot captures the whole database state for log compaction / catch-up.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	keys := f.db.Keys()
	rows := make([]snapshotRow, 0, len(keys))
	for _, k := range keys {
		ad, ok := f.db.LookupClassAd(k)
		if !ok {
			continue
		}
		rows = append(rows, snapshotRow{Key: k, AdText: ad.StringWithPrivate()})
	}
	return &fsmSnapshot{rows: rows}, nil
}

// Restore replaces the FSM state from a snapshot: it clears the database and
// loads every captured ad.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var rows []snapshotRow
	if err := json.NewDecoder(rc).Decode(&rows); err != nil {
		return fmt.Errorf("consistent: decoding snapshot: %w", err)
	}
	// Clear the existing keyspace.
	if keys := f.db.Keys(); len(keys) > 0 {
		tx := f.db.Begin()
		for _, k := range keys {
			tx.DestroyClassAd(k)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("consistent: clearing before restore: %w", err)
		}
	}
	// Load the snapshot.
	tx := f.db.Begin()
	for _, r := range rows {
		ad, err := classad.Parse(r.AdText)
		if err != nil {
			tx.Abort()
			return fmt.Errorf("consistent: restoring %s: %w", r.Key, err)
		}
		tx.NewClassAd(r.Key, ad)
	}
	return tx.Commit()
}

// fsmSnapshot is a point-in-time capture that raft persists asynchronously.
type fsmSnapshot struct {
	rows []snapshotRow
}

// Persist writes the snapshot to sink as JSON.
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		if err := json.NewEncoder(sink).Encode(s.rows); err != nil {
			return err
		}
		return sink.Close()
	}()
	if err != nil {
		_ = sink.Cancel()
	}
	return err
}

// Release is a no-op: the snapshot holds only an in-memory copy.
func (s *fsmSnapshot) Release() {}
