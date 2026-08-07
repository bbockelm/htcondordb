package historyimport

import (
	"context"
	"fmt"
	"sync"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// DBWriter implements Writer over a dbrpc client: the out-of-process importer
// appends records to the daemon's archive table over CEDAR, and the recovery
// dedup Has() is a GlobalJobId archive query. It creates the target table on
// first use so an importer can be pointed at a not-yet-existing table.
type DBWriter struct {
	Client *dbrpc.Client

	mu      sync.Mutex
	ensured map[string]bool
}

// ensure makes sure the archive table exists (idempotent, cached per table).
func (d *DBWriter) ensure(ctx context.Context, table string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.ensured[table] {
		return nil
	}
	tables, err := d.Client.ArchiveTables(ctx)
	if err != nil {
		return err
	}
	exists := false
	for _, t := range tables {
		if t == table {
			exists = true
			break
		}
	}
	if !exists {
		if err := d.Client.CreateArchiveTable(ctx, table, archiveConfig()); err != nil {
			// A concurrent importer may have created it between the list and here;
			// tolerate that by re-checking rather than failing the cycle.
			again, lerr := d.Client.ArchiveTables(ctx)
			if lerr != nil {
				return err
			}
			found := false
			for _, t := range again {
				if t == table {
					found = true
					break
				}
			}
			if !found {
				return err
			}
		}
	}
	if d.ensured == nil {
		d.ensured = map[string]bool{}
	}
	d.ensured[table] = true
	return nil
}

// Append adds a record to the table (old-ClassAd wire form over dbrpc).
func (d *DBWriter) Append(ctx context.Context, table string, ad *classad.ClassAd) error {
	if err := d.ensure(ctx, table); err != nil {
		return err
	}
	return d.Client.ArchiveAppend(ctx, table, ad.MarshalOld())
}

// Has reports whether the table already holds a record with this GlobalJobId.
func (d *DBWriter) Has(ctx context.Context, table, gid string) (bool, error) {
	if err := d.ensure(ctx, table); err != nil {
		return false, err
	}
	rows, err := d.Client.ArchiveQuery(ctx, table, fmt.Sprintf("GlobalJobId == %q", gid), 1)
	if err != nil {
		return false, err
	}
	return len(rows) > 0, nil
}
