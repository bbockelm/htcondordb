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
// The floor is exposed three ways: in each POST /ack response, logged per served table by the loop
// below (so an operator sees "what is safe to GC"), and -- when HTCONDORDB_CHANGEFEED_AGE_ATTR is
// set -- enforced as a runtime GC watermark via db.ArchiveTable.SetGCFloor so an archive's early-GC
// drain stops rotating consumed data past the min ack over live subscribers. Enforcement requires
// coupling each served archive's Retention.MinAgeAttr to the change-feed age attribute (done at
// startup); the optional HTCONDORDB_CHANGEFEED_MIN_AGE_SECONDS sets a guaranteed drain window
// (Retention.MinAge) before consumed data may be dropped. Hard ceilings (MaxAge/MaxSegments/
// MaxBytes) still win over the floor, so a slow or absent consumer never grows the store unbounded.
func startChangeFeed(ctx context.Context, d *daemon.Daemon, cfg *config.Config, svc *server.Service) {
	addr := getStr(cfg, "HTCONDORDB_CHANGEFEED_ADDRESS")
	if addr == "" {
		return
	}
	log := d.Logger()
	token := getStr(cfg, "HTCONDORDB_CHANGEFEED_TOKEN")
	ageAttr := getStr(cfg, "HTCONDORDB_CHANGEFEED_AGE_ATTR")

	// The GC floor only drains a served archive when its Retention.MinAgeAttr resolves to a
	// zone-mapped attribute -- so couple each archive's MinAgeAttr to the change-feed age attribute
	// (the same attribute the feed stamps as each event's GC timestamp). Without HTCONDORDB_
	// CHANGEFEED_AGE_ATTR the floor cannot be enforced; keep the log-only behavior below.
	if ageAttr != "" {
		minAge := configInt(cfg, "HTCONDORDB_CHANGEFEED_MIN_AGE_SECONDS")
		for _, name := range svc.Catalog().ArchiveTables() {
			at, ok := svc.Catalog().ArchiveTable(name)
			if !ok {
				continue
			}
			r := at.Retention()
			changed := false
			if r.MinAgeAttr != ageAttr {
				r.MinAgeAttr = ageAttr
				changed = true
			}
			if minAge > 0 && r.MinAge != float64(minAge) {
				r.MinAge = float64(minAge)
				changed = true
			}
			if changed {
				if err := at.SetRetention(r); err != nil {
					log.Warn(logging.DestinationGeneral, "change feed: could not couple archive retention to age attr",
						"table", name, "age_attr", ageAttr, "err", err.Error())
				}
			}
		}
	} else {
		log.Warn(logging.DestinationGeneral, "change feed: GC floor enforcement disabled; set HTCONDORDB_CHANGEFEED_AGE_ATTR to enable", "addr", addr)
	}

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
		AgeAttr:  ageAttr,
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

	// Reap expired subscriber leases, surface the per-table GC floor for observability, and (when
	// ageAttr is set) enforce it as each served archive's runtime GC watermark.
	go changeFeedFloorLoop(ctx, d, svc, reg, ageAttr)
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

// changeFeedFloorLoop periodically evicts dead subscribers, logs each served table's GC floor (min
// ack over live subscribers) -- so an operator knows what an archive could safely rotate -- and,
// when ageAttr is set, installs that floor as the archive's runtime GC watermark via SetGCFloor.
// The registry reports the floor in unix millis (ageAttrValue_seconds * 1000); SetGCFloor expects
// the archive's MinAgeAttr native units (seconds), so divide by 1000. When no live subscriber holds
// a floor the watermark is cleared (0) so retention falls back to its configured ceilings.
func changeFeedFloorLoop(ctx context.Context, d *daemon.Daemon, svc *server.Service, reg *changefeed.MemRegistry, ageAttr string) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			reg.Evict(now)
			for _, name := range svc.Catalog().ArchiveTables() {
				floor, held := reg.Floor(name, now)
				if held {
					d.Logger().Info(logging.DestinationGeneral, "change-feed GC floor",
						"table", name, "floor_unix", floor/1000, "subscribers", len(reg.Subscribers(name, now)))
				}
				if ageAttr == "" {
					continue
				}
				at, ok := svc.Catalog().ArchiveTable(name)
				if !ok {
					continue
				}
				if held {
					at.SetGCFloor(float64(floor) / 1000)
				} else {
					at.SetGCFloor(0)
				}
			}
		}
	}
}
