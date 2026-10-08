package federate

import (
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"

	"github.com/PelicanPlatform/classad/db"
)

// Index sets of the hub's tables. ScheddName is on every one: per-AP reads and the Reset sweep
// select on it. GlobalJobId is categorical, not a value index: value indexes are numeric, and the
// archive dedup probe is a string equality on it.
var (
	jobsCategorical    = []string{ScheddNameAttr, "User"}
	jobsValue          = []string{"JobStatus"}
	archiveCategorical = []string{ScheddNameAttr, "Owner", "User", "GlobalJobId"}
	archiveValue       = []string{"ClusterId"}
	historyZones       = []string{"CompletionDate", "EnteredHistoryTime"}
	epochZones         = []string{"EpochWriteDate", "EnteredHistoryTime"}
)

// ArchiveOptions tunes the hub's archives. Zero values leave the library defaults.
type ArchiveOptions struct {
	// ExtraCategorical names more categorical indexes (HTCONDORDB_ARCHIVE_CATEGORICAL_ATTRS).
	ExtraCategorical []string
	// ExtraValue names more value indexes (HTCONDORDB_ARCHIVE_VALUE_ATTRS).
	ExtraValue []string
	// SegmentSize applies only when an archive is created.
	SegmentSize int
	// MaxBytes caps each archive's on-disk size (0 = no cap), keyed by table name. The daemon's
	// periodic archive maintenance drops the oldest whole segments past it. Hub archives grow with
	// the sum of every AP's completions, so a production hub should set this.
	MaxBytes map[string]int64
}

// hubTables are the hub's table handles.
type hubTables struct {
	mutable  map[string]*db.DB
	archives map[string]*db.ArchiveTable
	sources  *db.DB
}

// ensureTables creates (or opens) the hub's tables and brings their index sets up to the required
// ones. Index additions on an existing archive are backfilled in the background on wg (the caller
// joins it before closing the catalog); a backfilling archive answers correctly, just slower.
func ensureTables(cat *db.Catalog, tables []string, opts ArchiveOptions, log *slog.Logger, wg *sync.WaitGroup) (*hubTables, error) {
	ht := &hubTables{mutable: map[string]*db.DB{}, archives: map[string]*db.ArchiveTable{}}
	src, err := cat.CreateTable(TableSources)
	if err != nil {
		return nil, fmt.Errorf("federate: creating %s: %w", TableSources, err)
	}
	ht.sources = src
	for _, t := range tables {
		switch {
		case isArchiveTable(t):
			vals := union(archiveValue, opts.ExtraValue)
			// An attribute may carry one kind of index, not both (the store panics on overlap).
			cats := slices.DeleteFunc(union(archiveCategorical, opts.ExtraCategorical), func(c string) bool {
				return slices.ContainsFunc(vals, func(v string) bool { return strings.EqualFold(c, v) })
			})
			zones := historyZones
			if t == TableEpochHistory {
				zones = epochZones
			}
			a, err := cat.CreateArchiveTable(t, db.ArchiveConfig{
				SegmentSize: opts.SegmentSize, CategoricalAttrs: cats, ValueAttrs: vals, ZoneAttrs: zones,
			})
			if err != nil {
				return nil, fmt.Errorf("federate: creating archive %s: %w", t, err)
			}
			// archiveconfig.json is authoritative on reopen; bring an existing archive's indexes
			// up to the required set (add-only), in the background.
			if addCat, addVal := missingIndexes(a.IndexedAttrs, cats, vals); len(addCat)+len(addVal) > 0 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					log.Info("federate: backfilling archive indexes", "archive", t, "categorical", addCat, "value", addVal)
					a.AddIndex(addCat, addVal)
				}()
			}
			if maxBytes := opts.MaxBytes[t]; a.Retention().MaxBytes != maxBytes {
				r := a.Retention()
				r.MaxBytes = maxBytes
				if err := a.SetRetention(r); err != nil {
					log.Error("federate: setting archive size limit", "archive", t, "max_bytes", maxBytes, "err", err)
				}
			}
			ht.archives[t] = a
		default:
			d, err := cat.CreateTable(t)
			if err != nil {
				return nil, fmt.Errorf("federate: creating %s: %w", t, err)
			}
			if t == TableJobs {
				if addCat, addVal := missingIndexes(d.IndexedAttrs, jobsCategorical, jobsValue); len(addCat)+len(addVal) > 0 {
					d.AddIndex(addCat, addVal)
				}
			}
			ht.mutable[t] = d
		}
	}
	return ht, nil
}

func missingIndexes(have func() ([]string, []string), wantCat, wantVal []string) (addCat, addVal []string) {
	hc, hv := have()
	miss := func(want, have []string) []string {
		var out []string
		for _, w := range want {
			if !slices.ContainsFunc(have, func(h string) bool { return strings.EqualFold(h, w) }) {
				out = append(out, w)
			}
		}
		return out
	}
	return miss(wantCat, hc), miss(wantVal, hv)
}

func union(a, b []string) []string {
	out := append([]string(nil), a...)
	for _, x := range b {
		if !slices.ContainsFunc(out, func(y string) bool { return strings.EqualFold(x, y) }) {
			out = append(out, x)
		}
	}
	return out
}
