// Package main builds a C client for the htcondordb DB server as a C archive: it exports C
// symbols (hcdb_*) that connect to an htcondordb daemon over an authenticated CEDAR session
// and run constraint queries or SQL, so a C/C++ daemon (e.g. condor_collector) can use the
// remote store the same way libclassad_db (db/capi) exposes the embedded one, and a
// non-Go client (e.g. the Python DB-API driver in python/) can reuse the whole stack.
// Build:
//
//	go build -buildmode=c-archive -o libhtcondordb_client.a ./capi  # C/C++ callers
//	go build -buildmode=c-shared  -o libhtcondordb_client.so ./capi # dlopen callers
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
// stdint.h for uintptr_t: glibc does not pull it in via stdlib.h the way macOS does, so
// without this the package builds on a Mac and fails on Linux with "could not determine
// what C.uintptr_t refers to".
#include <stdint.h>
*/
import "C"

import (
	"context"
	"fmt"
	"os"
	"runtime/cgo"
	"strings"
	"sync"
	"time"
	"unsafe"

	cedarclient "github.com/bbockelm/cedar/client"
	"github.com/bbockelm/cedar/security"
	htcondor "github.com/bbockelm/golang-htcondor"
	"github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/htcondordb/command"
	"github.com/bbockelm/htcondordb/repl"

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
//
// ex is the SQL executor (see hcdb_sql), built once per connection because it caches the
// server's archive-table set. mu serializes statement execution on the connection: the
// executor is not safe for concurrent use, and a caller with several cursors on one
// connection (the Python driver's normal shape) would otherwise race on that cache.
type conn struct {
	// addr is the address actually connected to, which is not necessarily the one the
	// caller passed: an empty address is resolved from the ambient configuration, and a
	// caller reporting "connected to ..." needs the answer rather than the blank.
	addr   string
	cl     *cedarclient.HTCondorClient
	dbc    *dbrpc.Client
	ex     *repl.Executor
	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
}

// openConn dials and authenticates a session, returning the error so callers that can
// surface a reason (hcdb_connect_err) do, and callers that cannot (hcdb_connect) drop it.
// resolveAddr passes a caller-supplied address through untouched, and locates the local
// daemon when the caller gave none -- the same way htcondordb-cli does with no -addr: the
// address file named by HTCONDORDB_ADDRESS_FILE (by default $(LOG)/.htcondordb_address),
// else the HTCONDORDB_HOST knob. The mechanics live in golang-htcondor so every client
// resolves htcondordb identically; a caller should not have to know a port to reach the
// daemon on its own machine.
//
// Resolution happens once per connection, as in the CLI. A client that wants to follow a
// restarted daemon reconnects, which comes back through here and re-reads the file.
func resolveAddr(cfg *config.Config, addr string) (string, error) {
	if strings.TrimSpace(addr) != "" {
		return addr, nil
	}
	resolve, source, err := htcondor.LocalDaemonAddress(cfg, "HTCONDORDB")
	if err != nil {
		return "", err
	}
	located, err := resolve()
	if err != nil {
		return "", fmt.Errorf("cannot locate the htcondordb daemon via %s: %w (pass an address, or set HTCONDORDB_ADDRESS_FILE / HTCONDORDB_HOST)", source, err)
	}
	return located, nil
}

func openConn(addr string) (*conn, error) {
	// Run as subsystem TOOL (like C++ command-line clients) so operator config scoped with
	// a TOOL. prefix (e.g. TOOL.SEC_CLIENT_AUTHENTICATION_METHODS) is honored; a bare
	// config.New() leaves the subsystem empty and disables <SUBSYS>.PARAM resolution.
	cfg, err := config.NewWithOptions(config.ConfigOptions{Subsystem: "TOOL"})
	if err != nil {
		// Name what failed: an unadorned "open /path: no such file" from a connect() call
		// reads like the driver's own problem rather than a misdirected CONDOR_CONFIG.
		return nil, fmt.Errorf("reading the HTCondor configuration: %w", err)
	}
	sec, err := htcondor.GetSecurityConfig(cfg, command.DBSession, "CLIENT")
	if err != nil {
		return nil, err
	}
	sec.Command = command.DBSession
	// Prefer (not require) authentication: PREFERRED maps the client to its user for WRITE
	// where a method is available, and still connects read-only when none is. Mirrors
	// htcondordb-cli connectDB.
	if sec.Authentication == security.SecurityOptional {
		sec.Authentication = security.SecurityPreferred
	}

	// Whether we picked the address rather than the caller: it changes what a failure has
	// to say, since the caller cannot name an address it never supplied.
	located := strings.TrimSpace(addr) == ""
	addr, err = resolveAddr(cfg, addr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	connCtx, connCancel := context.WithTimeout(ctx, 30*time.Second)
	defer connCancel()
	cl, err := cedarclient.ConnectAndAuthenticate(connCtx, addr, sec)
	if err != nil {
		cancel()
		if located {
			return nil, fmt.Errorf("connecting to %s (located from the configuration): %w", addr, err)
		}
		return nil, err
	}
	dbc := dbrpc.NewClient(dbrpc.NewCedarConn(ctx, cl.GetStream()))
	return &conn{
		addr:   addr,
		cl:     cl,
		dbc:    dbc,
		ex:     repl.NewExecutor(dbc, repl.ExecConfig{}),
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// Opens an authenticated dbrpc session to the htcondordb daemon at addr (a sinful or
// host:port), or to the local daemon located from the configuration when addr is empty. The client security policy (pool token / FS / SSL, per configuration) is read
// from the ambient HTCondor configuration via CONDOR_CONFIG, so the caller supplies only the
// address -- just like htcondordb-cli. Returns an opaque connection handle, or 0 on error.
// Release it with hcdb_close.
//
//export hcdb_connect
func hcdb_connect(addr *C.char) C.uintptr_t {
	c, err := openConn(C.GoString(addr))
	if err != nil {
		return 0
	}
	return C.uintptr_t(cgo.NewHandle(c))
}

// hcdb_connect with the failure reason: identical on success, but on error writes the
// message to *err (a C string the caller frees with hcdb_free) and returns 0. Distinguishing
// "host unreachable" from "authentication failed" matters to an interactive client, and
// hcdb_connect's signature cannot carry it.
//
//export hcdb_connect_err
func hcdb_connect_err(addr *C.char, err **C.char) C.uintptr_t {
	c, e := openConn(C.GoString(addr))
	if e != nil {
		*err = C.CString(e.Error())
		return 0
	}
	*err = nil
	return C.uintptr_t(cgo.NewHandle(c))
}

// handleConn resolves a connection handle, returning nil rather than panicking on a zero,
// stale, or wrong-typed one. cgo.Handle.Value panics in all three cases, and a panic that
// unwinds out of an exported function takes the host process with it -- fatal for a caller
// like the Python driver, where a use-after-close is an ordinary bug to report as an error.
func handleConn(h C.uintptr_t) (c *conn) {
	defer func() {
		if recover() != nil {
			c = nil
		}
	}()
	c, _ = cgo.Handle(h).Value().(*conn)
	return c
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
	c := handleConn(h)
	if c == nil {
		return 0
	}
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
	c := handleConn(h)
	if c == nil {
		return // already closed, or never valid: closing twice is not an error
	}
	_ = c.dbc.Close()
	_ = c.cl.Close()
	c.cancel()
	cgo.Handle(h).Delete()
}

// Sets an environment variable inside the library, or removes it when value is NULL.
// Returns hcdbOK, or hcdbErr for a NULL name.
//
// This exists because Go's os package answers Getenv from a copy of the environment made
// when the library is loaded: a variable the host process sets afterwards -- Python's
// os.environ["CONDOR_CONFIG"] = ... -- is invisible to the HTCondor configuration parser,
// which then silently reads the wrong configuration. A host that lets its user change the
// environment must push those changes in through here before connecting.
//
//export hcdb_setenv
func hcdb_setenv(name *C.char, value *C.char) C.int {
	if name == nil {
		return hcdbErr
	}
	key := C.GoString(name)
	if key == "" {
		return hcdbErr
	}
	if value == nil {
		if err := os.Unsetenv(key); err != nil {
			return hcdbErr
		}
		return hcdbOK
	}
	if err := os.Setenv(key, C.GoString(value)); err != nil {
		return hcdbErr
	}
	return hcdbOK
}

// Writes the address this connection is talking to -- resolved, if the caller passed an
// empty one -- to *out, a C string the caller frees with hcdb_free. Returns hcdbOK, or
// hcdbErr with the handle unusable.
//
//export hcdb_address
func hcdb_address(h C.uintptr_t, out **C.char) C.int {
	c := handleConn(h)
	if c == nil {
		*out = nil
		return hcdbErr
	}
	*out = C.CString(c.addr)
	return hcdbOK
}

// Frees a string returned by the library (e.g. hcdb_query_next).
//
//export hcdb_free
func hcdb_free(p *C.char) { C.free(unsafe.Pointer(p)) }
