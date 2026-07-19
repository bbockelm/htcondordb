// Package main builds a C client for the htcondordb DB server as a C archive: it exports C
// symbols (hcdb_*) that connect to an htcondordb daemon over an authenticated CEDAR session
// and run constraint queries, so a C/C++ daemon (e.g. condor_collector) can use the remote
// store the same way libclassad_db (db/capi) exposes the embedded one. Build:
//
//	go build -buildmode=c-archive -o libhtcondordb_client.a ./capi
//
// which also emits capi.h with these signatures.
//
// Handles: a connection and a query cursor are passed to C as opaque cgo.Handle values
// (uintptr_t). C never dereferences them; it only passes them back. Returned strings are
// C-allocated and must be released with hcdb_free.
//
// Authentication and transport are HTCondor's: hcdb_connect builds the client security
// policy from the ambient configuration (CONDOR_CONFIG) exactly as htcondordb-cli's
// connectDB does, then dials + authenticates a CEDAR session and multiplexes dbrpc over it.
package main

/*
#include <stdlib.h>
*/
import "C"

import (
	"context"
	"runtime/cgo"
	"time"
	"unsafe"

	cedarclient "github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/security"
	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/htcondordb/command"

	"github.com/PelicanPlatform/classad/dbrpc"
)

func main() {}

// Result codes shared with the C side (see capi.h).
const (
	hcdbOK      = 0
	hcdbErr     = -1
	hcdbMissing = -2
)

// conn bundles the authenticated CEDAR connection and the dbrpc client multiplexed over it,
// plus the context cancel that tears the session down.
type conn struct {
	cl     *cedarclient.HTCondorClient
	dbc    *dbrpc.Client
	ctx    context.Context
	cancel context.CancelFunc
}

// Opens an authenticated dbrpc session to the htcondordb daemon at addr (a sinful or
// host:port). The client security policy (pool token / FS / SSL, per configuration) is read
// from the ambient HTCondor configuration via CONDOR_CONFIG, so the caller supplies only the
// address -- just like htcondordb-cli. Returns an opaque connection handle, or 0 on error.
// Release it with hcdb_close.
//
//export hcdb_connect
func hcdb_connect(addr *C.char) C.uintptr_t {
	// Run as subsystem TOOL (like C++ command-line clients) so operator config scoped with
	// a TOOL. prefix (e.g. TOOL.SEC_CLIENT_AUTHENTICATION_METHODS) is honored; a bare
	// config.New() leaves the subsystem empty and disables <SUBSYS>.PARAM resolution.
	cfg, err := config.NewWithOptions(config.ConfigOptions{Subsystem: "TOOL"})
	if err != nil {
		return 0
	}
	sec, err := htcondor.GetSecurityConfig(cfg, command.DBSession, "CLIENT")
	if err != nil {
		return 0
	}
	sec.Command = command.DBSession
	// Prefer (not require) authentication: PREFERRED maps the client to its user for WRITE
	// where a method is available, and still connects read-only when none is. Mirrors
	// htcondordb-cli connectDB.
	if sec.Authentication == security.SecurityOptional {
		sec.Authentication = security.SecurityPreferred
	}

	ctx, cancel := context.WithCancel(context.Background())
	connCtx, connCancel := context.WithTimeout(ctx, 30*time.Second)
	defer connCancel()
	cl, err := cedarclient.ConnectAndAuthenticate(connCtx, C.GoString(addr), sec)
	if err != nil {
		cancel()
		return 0
	}
	dbc := dbrpc.NewClient(dbrpc.NewCedarConn(ctx, cl.GetStream()))
	return C.uintptr_t(cgo.NewHandle(&conn{cl: cl, dbc: dbc, ctx: ctx, cancel: cancel}))
}

// rows is a materialized query result the C side drains with hcdb_query_next.
type rows struct {
	ads []string
	i   int
}

// Runs a constraint query against the named table (empty/NULL = the default table) on the
// server and returns an opaque cursor over the matching ads' text, or 0 on error. The
// constraint is a ClassAd expression (the same text a query ad's Requirements holds). Drain
// with hcdb_query_next; free with hcdb_query_free.
//
//export hcdb_query
func hcdb_query(h C.uintptr_t, table, constraint *C.char) C.uintptr_t {
	c := cgo.Handle(h).Value().(*conn)
	var (
		ads []string
		err error
	)
	if t := C.GoString(table); t == "" {
		ads, err = c.dbc.Query(c.ctx, C.GoString(constraint))
	} else {
		ads, err = c.dbc.QueryTable(c.ctx, t, C.GoString(constraint), 0)
	}
	if err != nil {
		return 0
	}
	return C.uintptr_t(cgo.NewHandle(&rows{ads: ads}))
}

// Writes the next matching ad's text to *out -- a C string the caller frees with hcdb_free --
// and returns hcdbOK. Returns hcdbMissing when the cursor is exhausted (*out left NULL).
//
//export hcdb_query_next
func hcdb_query_next(qh C.uintptr_t, out **C.char) C.int {
	r := cgo.Handle(qh).Value().(*rows)
	if r.i >= len(r.ads) {
		*out = nil
		return hcdbMissing
	}
	*out = C.CString(r.ads[r.i])
	r.i++
	return hcdbOK
}

// Frees a query cursor handle.
//
//export hcdb_query_free
func hcdb_query_free(qh C.uintptr_t) { cgo.Handle(qh).Delete() }

// Closes the dbrpc session and the underlying CEDAR connection, and frees the handle.
//
//export hcdb_close
func hcdb_close(h C.uintptr_t) {
	hd := cgo.Handle(h)
	c := hd.Value().(*conn)
	_ = c.dbc.Close()
	_ = c.cl.Close()
	c.cancel()
	hd.Delete()
}

// Frees a string returned by the library (e.g. hcdb_query_next).
//
//export hcdb_free
func hcdb_free(p *C.char) { C.free(unsafe.Pointer(p)) }
