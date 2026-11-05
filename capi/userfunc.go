package main

import (
	"context"
	"log/slog"
	"sync"

	config "github.com/bbockelm/golang-htcondor/config"

	"github.com/bbockelm/htcondordb/userfunc"
)

// User functions in the client library.
//
// A query is evaluated in two places. A WHERE clause is sent to the server as an expression and
// evaluated there; a SELECT item is computed HERE, over the records that came back. So a function
// loaded only into the daemon works in one half of a query and is unknown in the other, and the
// person writing the SQL has no way to know which half they are in. This library loads the same
// modules from the same configuration, for the same reason htcondordb-cli does.

// userFuncs is process-wide because the registries it writes into are. One manager rather than one
// per connection: Apply only ever ADDS, so calling it again on a second connection is the no-op it
// looks like, where a fresh manager would try to register an already-registered name and fail.
var (
	userFuncOnce sync.Mutex
	userFuncs    *userfunc.Manager
)

// loadUserFuncs registers the configured modules, reporting a failure without failing the
// connection: a module that will not load breaks the queries that call it, and refusing to connect
// would break the ones that do not. It is safe to call on every connection.
func loadUserFuncs(cfg *config.Config) {
	userFuncOnce.Lock()
	defer userFuncOnce.Unlock()
	if userFuncs == nil {
		userFuncs = userfunc.New(context.Background(), slog.Default())
	}
	if err := userFuncs.Apply(cfg); err != nil {
		slog.Warn("user functions not loaded; a query calling one will fail in its SELECT items",
			"err", err.Error())
	}
}
