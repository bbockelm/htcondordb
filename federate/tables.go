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
	// SampleTime is what every metrics read ranges over, and what an age cap measures against
	// (MaxAgeAttr must be zone-mapped).
	jobMetricsZones = []string{"SampleTime"}
)

// archiveZones is the zone-map set of a federated archive.
func archiveZones(table string) []string {
	switch table {
	case TableEpochHistory:
		return epochZones
	case TableJobMetrics:
		return jobMetricsZones
	}
	return historyZones
}

// HubTables are the tables a hub federating tables (DefaultTables when empty) writes in process:
// each federated table plus federation_sources. The daemon registers them as owned, so no remote
// client can write or drop them under the hub.
func HubTables(tables []string) []string {
	if len(tables) == 0 {
		tables = DefaultTables
	}
	return append([]string{TableSources}, tables...)
}

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
	// MaxAge caps an archive by age, in seconds against its floorAttr (SampleTime for
	// job_metrics), keyed by table name; 0 = no age cap. A table absent from the map keeps
	// whatever age cap it has.
	MaxAge map[string]float64
	// SchemaRequireAttrs names the attributes the columnar schema must carry whatever their
	// presence rate (HTCONDORDB_SCHEMA_REQUIRE_ATTRS). A hub's history holds the same job ads as
	// a spoke's and is the larger of the two, so the attributes a job only acquires by running
	// are kept out of its schema by the same presence floor, for the same reason.
	SchemaRequireAttrs []string
	// Storage replaces ExtraCategorical, ExtraValue and SegmentSize for the tables it names, so an
	// archive can be stored as its spoke stores it (job_metrics takes the spoke's
	// HTCONDORDB_JOB_METRICS_* knobs, not the history ones).
	Storage map[string]TableStorage
}

// TableStorage is one archive's storage settings (see ArchiveOptions.Storage). The hub's own
// indexes are always added to the extras.
type TableStorage struct {
	ExtraCategorical []string
	ExtraValue       []string
	// SegmentSize and GroupSchemaCount apply only when the archive is created: 0 is the library
	// default; a negative GroupSchemaCount builds no group schemas.
	SegmentSize      int
	GroupSchemaCount int
}

// storage is table's storage settings.
func (o ArchiveOptions) storage(table string) TableStorage {
	if st, ok := o.Storage[table]; ok {
		return st
	}
	return TableStorage{ExtraCategorical: o.ExtraCategorical, ExtraValue: o.ExtraValue, SegmentSize: o.SegmentSize}
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
			st := opts.storage(t)
			vals := union(archiveValue, st.ExtraValue)
			// An attribute may carry one kind of index, not both (the store panics on overlap).
			cats := slices.DeleteFunc(union(archiveCategorical, st.ExtraCategorical), func(c string) bool {
				return slices.ContainsFunc(vals, func(v string) bool { return strings.EqualFold(c, v) })
			})
			// job_metrics is excluded: the sampler writes a fixed attribute set on every sample,
			// so its fields clear the presence floor by construction.
			var require []string
			if t != TableJobMetrics {
				require = opts.SchemaRequireAttrs
			}
			a, err := cat.CreateArchiveTable(t, db.ArchiveConfig{
				SegmentSize: st.SegmentSize, CategoricalAttrs: cats, ValueAttrs: vals, ZoneAttrs: archiveZones(t),
				GroupSchemaCount:   st.GroupSchemaCount,
				SchemaRequireAttrs: require,
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
			// As for the indexes: archiveconfig.json is authoritative on reopen, so the configured
			// set has to be re-asserted onto an archive that already exists. Nothing is rebuilt --
			// re-deriving the schema re-encodes one columnar block per sealed segment, which a
			// hub's history makes expensive; `.schema rebuild history` is how an operator pays it.
			if changed, err := a.SetSchemaRequireAttrs(require); err != nil {
				log.Error("federate: setting required schema attributes", "archive", t, "err", err)
			} else if changed {
				log.Info("federate: required schema attributes set", "archive", t, "attrs", require,
					"note", "run `.schema rebuild "+t+"` to re-derive the schema")
			}
			if maxBytes := opts.MaxBytes[t]; a.Retention().MaxBytes != maxBytes {
				r := a.Retention()
				r.MaxBytes = maxBytes
				if err := a.SetRetention(r); err != nil {
					log.Error("federate: setting archive size limit", "archive", t, "max_bytes", maxBytes, "err", err)
				}
			}
			if maxAge, ok := opts.MaxAge[t]; ok {
				setMaxAge(a, t, maxAge, log)
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

// setMaxAge sets a's age cap to maxAge seconds against table's floorAttr, if it differs.
func setMaxAge(a *db.ArchiveTable, table string, maxAge float64, log *slog.Logger) {
	r := a.Retention()
	attr := floorAttr(table)
	if r.MaxAge == maxAge && (maxAge == 0 || r.MaxAgeAttr == attr) {
		return
	}
	r.MaxAge = maxAge
	if maxAge > 0 {
		r.MaxAgeAttr = attr
	}
	if err := a.SetRetention(r); err != nil {
		log.Error("federate: setting archive age limit", "archive", table, "max_age_seconds", maxAge, "err", err)
	}
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
