package server

import (
	"slices"
	"strings"
	"sync"
	"sync/atomic"
)

// TableOwners records which tables belong to a writer running inside this daemon (schedd sync,
// cedar-sync replication, a managed history importer). An owned table is read-only to every
// remote peer, DAEMON included: the server hands dbrpc a TableWritable that consults this
// registry on every mutating request (see Service.ServeOptions), so a client can read and watch
// a mirror but cannot change it underneath the writer that maintains it.
//
// Ownership follows configuration: each manager calls Set with the tables its CURRENT writers
// own on every (re)apply, and Set(owner, nil) when it is disabled or stopped, so a table stops
// being owned the moment its writer goes away.
//
// A writer that runs out of process (the history importer) reaches the daemon over a minted
// CEDAR session; GrantSession lets connections resumed from that session write the tables its
// owner holds, and nothing else does.
//
// Table names are matched case-insensitively. The catalog itself is case-sensitive, but a
// persistent table is a directory named after it, and on a case-insensitive filesystem (the
// macOS default) "History" opens the same files as "history" -- so a differently-cased name must
// not be a way around the gate.
//
// The zero value is not usable; use NewTableOwners. A nil *TableOwners owns nothing.
type TableOwners struct {
	mu       sync.Mutex
	byOwner  map[string][]string // owner -> the tables it holds, as given
	sessions map[string]string   // CEDAR session id -> the owner it writes for

	snap atomic.Pointer[ownersSnapshot] // read lock-free from request goroutines
}

type ownersSnapshot struct {
	tables   map[string][]string // folded table name -> sorted owners
	sessions map[string]string
}

// NewTableOwners returns an empty registry.
func NewTableOwners() *TableOwners {
	o := &TableOwners{byOwner: map[string][]string{}, sessions: map[string]string{}}
	o.snap.Store(&ownersSnapshot{tables: map[string][]string{}, sessions: map[string]string{}})
	return o
}

func foldTable(name string) string { return strings.ToLower(name) }

// Set replaces the set of tables owner holds. An empty list releases all of them.
func (o *TableOwners) Set(owner string, tables []string) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(tables) == 0 {
		delete(o.byOwner, owner)
	} else {
		o.byOwner[owner] = slices.Clone(tables)
	}
	o.publish()
}

// GrantSession lets connections on CEDAR session id write the tables owner holds. Used for an
// out-of-process writer the daemon launches with a minted session.
func (o *TableOwners) GrantSession(id, owner string) {
	if o == nil || id == "" {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.sessions[id] = owner
	o.publish()
}

// RevokeSession withdraws a GrantSession. A connection still open on the session loses the
// grant for its next write.
func (o *TableOwners) RevokeSession(id string) {
	if o == nil || id == "" {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.sessions, id)
	o.publish()
}

// publish rebuilds the read snapshot. Caller holds o.mu.
func (o *TableOwners) publish() {
	s := &ownersSnapshot{tables: map[string][]string{}, sessions: make(map[string]string, len(o.sessions))}
	for owner, tables := range o.byOwner {
		for _, t := range tables {
			k := foldTable(t)
			if !slices.Contains(s.tables[k], owner) {
				s.tables[k] = append(s.tables[k], owner)
			}
		}
	}
	for k := range s.tables {
		slices.Sort(s.tables[k])
	}
	for id, owner := range o.sessions {
		s.sessions[id] = owner
	}
	o.snap.Store(s)
}

// Owners returns the owners of table, sorted; nil when it is not owned.
func (o *TableOwners) Owners(table string) []string {
	if o == nil {
		return nil
	}
	return slices.Clone(o.snap.Load().tables[foldTable(table)])
}

// Owned reports whether any in-process writer owns table.
func (o *TableOwners) Owned(table string) bool {
	if o == nil {
		return false
	}
	return len(o.snap.Load().tables[foldTable(table)]) > 0
}

// Writable reports whether a remote connection on CEDAR session sessionID may modify table: an
// unowned table always, an owned one only from a session granted to one of its owners. Safe for
// concurrent use and never blocks (dbrpc calls it on every mutating request).
func (o *TableOwners) Writable(table, sessionID string) bool {
	if o == nil {
		return true
	}
	s := o.snap.Load()
	owners := s.tables[foldTable(table)]
	if len(owners) == 0 {
		return true
	}
	if sessionID == "" {
		return false
	}
	grantee, ok := s.sessions[sessionID]
	return ok && slices.Contains(owners, grantee)
}
