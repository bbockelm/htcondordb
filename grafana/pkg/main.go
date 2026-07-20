// Command grafana-htcondordb-datasource is the backend of the htcondordb Grafana
// datasource plugin. Grafana launches it as a hashicorp go-plugin subprocess and
// talks to it over gRPC; datasource.Manage wires the instance factory to the SDK's
// serve loop.
package main

import (
	"os"

	"github.com/grafana/grafana-plugin-sdk-go/backend/datasource"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"

	"github.com/bbockelm/htcondordb/grafana/pkg/plugin"
)

// pluginID must match the "id" in src/plugin.json.
const pluginID = "bbockelm-htcondordb-datasource"

func main() {
	if err := datasource.Manage(pluginID, plugin.NewDatasource, datasource.ManageOpts{}); err != nil {
		log.DefaultLogger.Error("htcondordb datasource exited", "error", err.Error())
		os.Exit(1)
	}
}
