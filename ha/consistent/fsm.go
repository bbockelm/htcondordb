package consistent

import (
	"bytes"
	"fmt"
	"io"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/hashicorp/raft"
)

// FSM is the raft finite state machine: it applies committed mutation batches to a local
// ClassAd catalog (every table), so all replicas converge on identical multi-table state.
// A batch may touch several tables; the FSM groups its ops by table and applies each
// table's ops in that table's own transaction (deterministically on every replica).
type FSM struct {
	cat          *db.Catalog
	defaultTable string // table an op with an empty Table name targets
}

// NewFSM builds an FSM over cat. defaultTable is where table-unqualified ops apply
// (pre-multi-table log entries and single-table batches). The FSM is the catalog's sole
// writer in consistent mode.
func NewFSM(cat *db.Catalog, defaultTable string) *FSM {
	return &FSM{cat: cat, defaultTable: defaultTable}
}

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

// applyBatch applies a batch, grouping ops by table (each table's ops in one transaction).
// Order within a table is preserved; a batch that touches only one table is one atomic
// transaction, as before.
func (f *FSM) applyBatch(b *Batch) error {
	byTable := map[string][]Op{}
	var order []string
	for _, op := range b.Ops {
		t := op.Table
		if t == "" {
			t = f.defaultTable
		}
		if _, seen := byTable[t]; !seen {
			order = append(order, t)
		}
		byTable[t] = append(byTable[t], op)
	}
	for _, t := range order {
		d, err := f.cat.EnsureTable(t)
		if err != nil {
			return fmt.Errorf("consistent: ensuring table %q: %w", t, err)
		}
		if err := applyOps(d, byTable[t]); err != nil {
			return err
		}
	}
	return nil
}

// applyOps applies one table's ops in a single transaction.
func applyOps(d *db.DB, ops []Op) error {
	tx := d.Begin()
	for _, op := range ops {
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

// Snapshot captures the whole catalog (every table) for log compaction / catch-up. raft
// calls this without a concurrent Apply, so the buffered capture is a consistent point in
// time; Persist (which may run concurrently with later Applies) just writes the buffer.
// The capture reuses the catalog's own multi-table, encryption-aware snapshot format --
// so an encrypted node's raft snapshots are encrypted at rest too (a peer restores it via
// the shared pool keys, exactly like a backup).
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	var buf bytes.Buffer
	if err := f.cat.Snapshot(&buf); err != nil {
		return nil, fmt.Errorf("consistent: capturing snapshot: %w", err)
	}
	return &fsmSnapshot{data: buf.Bytes()}, nil
}

// Restore replaces the whole catalog from a snapshot (each table truncated + reloaded).
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	if err := f.cat.Restore(rc); err != nil {
		return fmt.Errorf("consistent: restoring snapshot: %w", err)
	}
	return nil
}

// fsmSnapshot is a point-in-time capture (a serialized catalog snapshot) that raft
// persists asynchronously.
type fsmSnapshot struct {
	data []byte
}

// Persist writes the captured snapshot bytes to sink.
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

// Release is a no-op: the snapshot holds only an in-memory copy.
func (s *fsmSnapshot) Release() {}
