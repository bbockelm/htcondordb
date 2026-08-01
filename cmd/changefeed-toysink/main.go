// Command changefeed-toysink is a minimal, standalone, NON-CEDAR consumer of an htcondordb change
// feed: it pulls the HTTP/SSE feed (github.com/PelicanPlatform/classad/changefeed) and applies each
// change into a local classad/db archive -- i.e. it "inserts into a destination htcondordb store"
// without ever speaking CEDAR. It exposes GET /count (records applied so far) and GET /healthz, and
// shuts down cleanly on SIGINT/SIGTERM (draining the current apply). It is the reference sink used
// by the changefeed integration test and a template for a real fan-in sink project.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/PelicanPlatform/classad/changefeed"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/db/replicate"
)

func main() {
	source := flag.String("source", "", "changefeed base URL, e.g. http://host:9620 (required)")
	token := flag.String("token", "", "bearer token for the feed")
	table := flag.String("table", "history", "source table/archive to subscribe to")
	target := flag.String("target", "history", "local target archive name")
	subscriber := flag.String("subscriber", "toysink", "durable subscription id")
	src := flag.String("src", "toysink", "source label stamped on applied rows")
	constraint := flag.String("constraint", "", "optional server-side filter")
	dir := flag.String("dir", "", "local classad/db catalog directory (required)")
	listen := flag.String("listen", "127.0.0.1:0", "address for the /count HTTP endpoint")
	addrFile := flag.String("addr-file", "", "write the bound /count address here (for :0 discovery)")
	flag.Parse()
	if *source == "" || *dir == "" {
		log.Fatal("changefeed-toysink: -source and -dir are required")
	}

	cat, err := db.OpenCatalog(*dir)
	if err != nil {
		log.Fatalf("open catalog %s: %v", *dir, err)
	}
	defer cat.Close()
	arch, err := cat.CreateArchiveTable(*target, db.ArchiveConfig{ValueAttrs: []string{"ClusterId"}})
	if err != nil {
		log.Fatalf("create archive %s: %v", *target, err)
	}
	sink, err := replicate.NewArchiveSink(arch, *src, replicate.FileCursorStore{Path: filepath.Join(*dir, "cursor")})
	if err != nil {
		log.Fatalf("build sink: %v", err)
	}

	// /count lets an observer watch records land without opening the (single-writer) store.
	mux := http.NewServeMux()
	mux.HandleFunc("/count", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintf(w, "%d", arch.Count()) })
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "ok") })
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("listen %s: %v", *listen, err)
	}
	if *addrFile != "" {
		if err := os.WriteFile(*addrFile, []byte(ln.Addr().String()+"\n"), 0o644); err != nil {
			log.Fatalf("write addr file: %v", err)
		}
	}
	httpSrv := &http.Server{Handler: mux}
	go func() { _ = httpSrv.Serve(ln) }()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Printf("changefeed-toysink: pulling %s table=%s -> %s (%s)", *source, *table, *dir, ln.Addr())

	// Pull runs until the signal cancels ctx; it resumes from the persisted cursor on (re)start.
	err = changefeed.Pull(ctx, changefeed.PullConfig{
		BaseURL: *source, Table: *table, Subscriber: *subscriber, Src: *src,
		Constraint: *constraint, Token: *token, AckEvery: 500 * time.Millisecond,
	}, sink)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	if err != nil {
		log.Fatalf("pull: %v", err)
	}
	log.Print("changefeed-toysink: clean shutdown")
}
