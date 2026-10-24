package scheddsync

import (
	"context"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/db"
)

// durableCheckingStore is a PositionStore that, on every Save, requires every record in the
// archive to be durable. The archive publishes an append to its watchers only once a sync has
// made it durable, so a watcher that has seen every record is the observable form of "the
// archive has been flushed".
type durableCheckingStore struct {
	t     *testing.T
	arch  *db.ArchiveTable
	seen  *atomic.Int64
	saves int
	blob  []byte
}

func (s *durableCheckingStore) Save(blob []byte) error {
	want := int64(s.arch.Count())
	deadline := time.Now().Add(5 * time.Second)
	for s.seen.Load() < want && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := s.seen.Load(); got < want {
		s.t.Errorf("position saved while %d of %d archived records were not yet durable", want-got, want)
	}
	s.saves++
	s.blob = append(s.blob[:0], blob...)
	return nil
}

func (s *durableCheckingStore) Load() ([]byte, bool, error) {
	return s.blob, s.blob != nil, nil
}

// TestHistoryPositionSavedAfterFlush: records are appended deferred, so the saved position
// must follow a flush of the archive -- otherwise a machine crash leaves the position ahead
// of what the archive holds, and recovery, which resumes from it, never re-reads the records
// in between.
func TestHistoryPositionSavedAfterFlush(t *testing.T) {
	arch, cleanup := newArchive(t)
	defer cleanup()
	var seen atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	events, err := arch.Watch(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	synced := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ev := range events {
			switch ev.Kind {
			case db.WatchUpsert:
				seen.Add(1)
			case db.WatchSynced:
				close(synced)
			}
		}
	}()
	defer func() { cancel(); <-done }()
	<-synced

	dir := t.TempDir()
	histPath := filepath.Join(dir, "history")
	var b strings.Builder
	for c := 1; c <= 30; c++ {
		b.WriteString(histRecord(c, 0, 4))
	}
	writeFile(t, histPath, b.String())
	store := &durableCheckingStore{t: t, arch: arch, seen: &seen}
	s := NewHistorySync(arch, HistorySyncConfig{Filename: histPath, Store: store, Logger: quietLogger()})
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	b.Reset()
	for c := 31; c <= 50; c++ {
		b.WriteString(histRecord(c, 0, 4))
	}
	appendFile(t, histPath, b.String())
	if err := s.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if arch.Count() != 50 {
		t.Fatalf("archived %d records, want 50", arch.Count())
	}
	if store.saves < 2 {
		t.Fatalf("position saved %d times, want at least 2", store.saves)
	}
}
