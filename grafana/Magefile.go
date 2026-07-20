//go:build mage

package main

import (
	// mage:import
	build "github.com/grafana/grafana-plugin-sdk-go/build"
)

// Default is the mage target run by a bare `mage`: build backend binaries for all
// supported OS/arch into dist/ (gpx_htcondordb_<os>_<arch>), which Grafana loads
// per the "executable" in src/plugin.json.
var Default = build.BuildAll
