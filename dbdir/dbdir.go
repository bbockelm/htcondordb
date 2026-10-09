// Package dbdir resolves where an htcondordb keeps its data on disk.
//
// It is its own package because two very different programs need the same answer and
// must not disagree: the daemon, which creates the directory, and htcondordb-cli's
// fsck, which inspects it while the daemon is not running. A second copy of this rule
// would be a bug waiting for someone to change one knob and not the other.
package dbdir

import (
	"path/filepath"
	"strings"

	"github.com/bbockelm/golang-htcondor/config"
)

// Subdirectories of a database directory. A catalog keeps one directory per table
// under Tables, and one per append-only archive under Archives.
const (
	Tables   = "tables"
	Archives = "archives"
)

// Resolve returns the on-disk database directory: HTCONDORDB_DIR if set, else
// $(SPOOL)/htcondordb, else "" (the database is in-memory and has no directory).
//
// It is the single source of truth for the DB dir, so everything under it -- the
// catalog, the archives, and the sync position stores -- lands in the same place
// whichever knob is set.
func Resolve(cfg *config.Config) string {
	if cfg == nil {
		return ""
	}
	if v, ok := cfg.Get("HTCONDORDB_DIR"); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	if spool, ok := cfg.Get("SPOOL"); ok && strings.TrimSpace(spool) != "" {
		return filepath.Join(strings.TrimSpace(spool), "htcondordb")
	}
	return ""
}
