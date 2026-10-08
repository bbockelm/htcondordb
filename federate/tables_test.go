package federate

import (
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

func hasFold(list []string, want string) bool {
	return slices.ContainsFunc(list, func(s string) bool { return strings.EqualFold(s, want) })
}

// TestHubArchiveIndexes pins the categorical indexes readers rely on: the API server self-scopes
// every history read on User and narrows by ScheddName, and the dedup probe is on GlobalJobId.
func TestHubArchiveIndexes(t *testing.T) {
	cat := openCatalog(t, t.TempDir())
	defer cat.Close()
	var wg sync.WaitGroup
	ht, err := ensureTables(cat, []string{TableHistory, TableEpochHistory}, ArchiveOptions{}, discard, &wg)
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	for _, name := range []string{TableHistory, TableEpochHistory} {
		cats, _ := ht.archives[name].IndexedAttrs()
		for _, want := range []string{ScheddNameAttr, "Owner", "User", "GlobalJobId"} {
			if !hasFold(cats, want) {
				t.Errorf("%s: categorical indexes %v lack %s", name, cats, want)
			}
		}
	}
}

// TestHubArchiveIndexBackfill: a hub archive created before an index was required gains it on the
// next open, without being rebuilt.
func TestHubArchiveIndexBackfill(t *testing.T) {
	dir := t.TempDir()
	cat := openCatalog(t, dir)
	if _, err := cat.CreateArchiveTable(TableHistory, db.ArchiveConfig{
		CategoricalAttrs: []string{ScheddNameAttr, "Owner", "GlobalJobId"},
		ValueAttrs:       archiveValue,
		ZoneAttrs:        historyZones,
	}); err != nil {
		t.Fatal(err)
	}
	if err := cat.Close(); err != nil {
		t.Fatal(err)
	}

	cat = openCatalog(t, dir)
	defer cat.Close()
	var wg sync.WaitGroup
	ht, err := ensureTables(cat, []string{TableHistory}, ArchiveOptions{}, discard, &wg)
	if err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if cats, _ := ht.archives[TableHistory].IndexedAttrs(); !hasFold(cats, "User") {
		t.Errorf("reopened archive's categorical indexes %v lack User", cats)
	}
}
