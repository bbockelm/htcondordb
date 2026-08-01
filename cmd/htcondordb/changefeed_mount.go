package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/PelicanPlatform/classad/changefeed"
	"github.com/bbockelm/golang-htcondor/config"
	"github.com/bbockelm/golang-htcondor/daemon"
	"github.com/bbockelm/golang-htcondor/logging"

	"github.com/bbockelm/htcondordb/server"
)

// startChangeFeed serves the transport-neutral HTTP/SSE change feed
// (github.com/PelicanPlatform/classad/changefeed) so a standalone, non-CEDAR sink can pull and
// fan in this database's tables/archives. Opt-in via HTCONDORDB_CHANGEFEED_ADDRESS (e.g. ":9620");
// token-gated by HTCONDORDB_CHANGEFEED_TOKEN. HTCONDORDB_CHANGEFEED_AGE_ATTR (e.g.
// "EnteredHistoryTime") is stamped as each event's GC timestamp so a sink's ACK advances the
// source's GC floor.
//
// The floor is exposed two ways today: in each POST /ack response, and logged per served table by
// the loop below (so an operator sees "what is safe to GC"). Automatic retention enforcement --
// not rotating an archive below the floor -- is a follow-up needing a runtime
// db.ArchiveTable.SetRetainFloor(t) API (the "extend MaxAge" alternative ratchets across restarts
// and would silently void the operator's configured MaxAge).
func startChangeFeed(ctx context.Context, d *daemon.Daemon, cfg *config.Config, svc *server.Service) {
	addr := getStr(cfg, "HTCONDORDB_CHANGEFEED_ADDRESS")
	if addr == "" {
		return
	}
	log := d.Logger()
	token := getStr(cfg, "HTCONDORDB_CHANGEFEED_TOKEN")

	reg := &changefeed.MemRegistry{}
	if s := configInt(cfg, "HTCONDORDB_CHANGEFEED_LEASE_SECONDS"); s > 0 {
		reg.LeaseTTL = time.Duration(s) * time.Second
	}

	var auth changefeed.Authorizer
	if token != "" {
		auth = bearerAuthorizer(token)
	} else {
		log.Warn(logging.DestinationGeneral, "change feed served WITHOUT a token (set HTCONDORDB_CHANGEFEED_TOKEN)", "addr", addr)
	}

	handler := changefeed.Handler(svc.Catalog(), changefeed.ServerOptions{
		Auth:     auth,
		Registry: reg,
		AgeAttr:  getStr(cfg, "HTCONDORDB_CHANGEFEED_AGE_ATTR"),
	})
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Error(logging.DestinationGeneral, "change feed: cannot listen", "addr", addr, "err", err.Error())
		return
	}
	// Publish the actual bound address (covering ":0" dynamic ports) so tooling can discover it.
	if af := getStr(cfg, "HTCONDORDB_CHANGEFEED_ADDRESS_FILE"); af != "" {
		if werr := os.WriteFile(af, []byte(ln.Addr().String()+"\n"), 0o644); werr != nil {
			log.Warn(logging.DestinationGeneral, "change feed: could not write address file", "path", af, "err", werr.Error())
		}
	}
	srv := &http.Server{Handler: handler}
	log.Info(logging.DestinationGeneral, "serving change feed", "addr", ln.Addr().String(), "token", token != "")
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Error(logging.DestinationGeneral, "change feed listener stopped", "err", err.Error())
		}
	}()
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()

	// Reap expired subscriber leases and surface the per-table GC floor for observability.
	go changeFeedFloorLoop(ctx, d, svc, reg)
}

// bearerAuthorizer authorizes a change-feed request iff it carries the given bearer token, taking
// the src label and subscriber id from the query (the token only authorizes).
func bearerAuthorizer(token string) changefeed.Authorizer {
	want := "Bearer " + token
	return func(r *http.Request) (src, sub string, ok bool) {
		if r.Header.Get("Authorization") != want {
			return "", "", false
		}
		return r.URL.Query().Get("src"), r.URL.Query().Get("subscriber"), true
	}
}

// changeFeedFloorLoop periodically evicts dead subscribers and logs each served table's GC floor
// (min ack over live subscribers) -- so an operator knows what an archive could safely rotate.
func changeFeedFloorLoop(ctx context.Context, d *daemon.Daemon, svc *server.Service, reg *changefeed.MemRegistry) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			reg.Evict(now)
			for _, name := range svc.Catalog().ArchiveTables() {
				if floor, held := reg.Floor(name, now); held {
					d.Logger().Info(logging.DestinationGeneral, "change-feed GC floor",
						"table", name, "floor_unix", floor/1000, "subscribers", len(reg.Subscribers(name, now)))
				}
			}
		}
	}
}
