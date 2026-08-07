package main

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bbockelm/golang-htcondor/config"
)

func importerCfg(t *testing.T, body string) *config.Config {
	t.Helper()
	cfg, err := config.NewFromReaderWithOptions(strings.NewReader(body), config.ConfigOptions{Subsystem: "HTCONDORDB"})
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	return cfg
}

func TestResolveImporterSettings(t *testing.T) {
	t.Run("configured jobs enable the manager", func(t *testing.T) {
		s := resolveImporterSettings(importerCfg(t, `
HTCONDORDB_HISTORY_IMPORT = ospool cm_east
HTCONDORDB_HISTORY_IMPORT_OSPOOL_POOL = cm.osg-htc.org:9618
HTCONDORDB_HISTORY_IMPORT_CM_EAST_POOL = cm-east.example:9618
HTCONDORDB_HISTORY_IMPORT_USER = importer_svc
HTCONDORDB_HISTORY_IMPORT_SHUTDOWN_SECONDS = 45
`))
		if !s.enabled {
			t.Fatal("should be enabled with jobs configured")
		}
		if s.user != "importer_svc" {
			t.Errorf("user = %q, want importer_svc", s.user)
		}
		if s.gracefulTimeout != 45*time.Second {
			t.Errorf("grace = %v, want 45s", s.gracefulTimeout)
		}
		if !reflect.DeepEqual(s.jobs, []string{"cm_east", "ospool"}) { // sorted
			t.Errorf("jobs = %v, want [cm_east ospool]", s.jobs)
		}
	})

	t.Run("default user is condor", func(t *testing.T) {
		s := resolveImporterSettings(importerCfg(t, `
HTCONDORDB_HISTORY_IMPORT = j1
HTCONDORDB_HISTORY_IMPORT_J1_POOL = p:9618
`))
		if s.user != "condor" {
			t.Errorf("default user = %q, want condor", s.user)
		}
	})

	t.Run("no jobs disables", func(t *testing.T) {
		if s := resolveImporterSettings(importerCfg(t, ``)); s.enabled {
			t.Error("no jobs should leave the manager disabled")
		}
	})

	t.Run("MANAGE=false disables", func(t *testing.T) {
		s := resolveImporterSettings(importerCfg(t, `
HTCONDORDB_MANAGE_HISTORY_IMPORT = false
HTCONDORDB_HISTORY_IMPORT = j1
HTCONDORDB_HISTORY_IMPORT_J1_POOL = p:9618
`))
		if s.enabled {
			t.Error("HTCONDORDB_MANAGE_HISTORY_IMPORT=false should disable the manager")
		}
	})

	t.Run("a job missing POOL disables (invalid config)", func(t *testing.T) {
		s := resolveImporterSettings(importerCfg(t, `HTCONDORDB_HISTORY_IMPORT = j1`))
		if s.enabled {
			t.Error("an invalid import config should disable rather than enable")
		}
	})
}

func TestImporterSettingsEqual(t *testing.T) {
	a := importerSettings{enabled: true, user: "condor", jobs: []string{"a", "b"}, gracefulTimeout: time.Second}
	b := importerSettings{enabled: true, user: "condor", jobs: []string{"a", "b"}, gracefulTimeout: time.Second}
	if !a.equal(b) {
		t.Error("identical settings should be equal")
	}
	b.jobs = []string{"a", "c"}
	if a.equal(b) {
		t.Error("different job sets should not be equal (drives reconfigure restart)")
	}
}
