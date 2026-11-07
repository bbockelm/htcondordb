package bench

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// Standing up a benchmark server: generated data, in scratch, read-only and anonymous.
//
// THIS IS A SEPARATE BOOTSTRAP FROM THE DAEMON'S, deliberately. Benchmark mode opens a store with
// anonymous reads and an open Flight endpoint, and the one thing that must never happen is for
// those settings to meet a production configuration -- a SPOOL inherited from the environment, a
// pool password already on disk, an advertise address. Sharing the daemon's bootstrap and
// branching inside it would make that one missing condition away. Building the server here, from
// nothing but the scratch directory and these options, means production config cannot leak in
// because none of it is ever read.
//
// ANONYMOUS MEANS READ-ONLY. The peer is unauthenticated, so it gets ServeOptions.ReadOnly: it
// may read and snapshot, and cannot write, retune or restructure the store. There is nothing
// secret in a generated dataset, but an anonymous writer is a different thing from an anonymous
// reader and only the second was asked for.

// ServeOptions configures a benchmark server.
type ServeConfig struct {
	// Dir is the scratch directory. It is guarded before anything is opened.
	Dir string
	// Rows is how many ads to generate per table.
	Rows int
	// Shapes are the tables to build. Empty means all three a deployment serves.
	Shapes []Shape
	// Listen is the dbrpc address, "" for none. FlightListen is the Flight SQL address.
	Listen       string
	FlightListen string
	// Seed makes the generated dataset reproducible across machines.
	Seed int64
}

// Server is a running benchmark server.
type Server struct {
	Catalog *db.Catalog
	Addr    string
	Tables  map[Shape]*db.ArchiveTable
	stop    func()
}

// Close shuts the server down and closes the store.
func (s *Server) Close() {
	if s.stop != nil {
		s.stop()
	}
	if s.Catalog != nil {
		_ = s.Catalog.Close()
	}
}

// BuildReport says what was generated and what state it was left in, per table.
type BuildReport struct {
	Shape   Shape
	Rows    int
	Prepare PrepareReport
	Elapsed time.Duration
}

// Build creates the scratch store, generates each shape and brings it to steady state. It guards
// the directory FIRST: nothing is opened, and no endpoint is bound, until the directory is proven
// to be scratch.
func Build(cfg ServeConfig, progress io.Writer) (*Server, []BuildReport, error) {
	if err := GuardScratchDir(cfg.Dir); err != nil {
		return nil, nil, err
	}
	if cfg.Rows <= 0 {
		cfg.Rows = 100000
	}
	if len(cfg.Shapes) == 0 {
		cfg.Shapes = []Shape{ShapeJobs, ShapeHistory, ShapeEpochHistory}
	}
	cat, err := db.OpenCatalog(cfg.Dir)
	if err != nil {
		return nil, nil, fmt.Errorf("opening scratch catalog in %s: %w", cfg.Dir, err)
	}
	srv := &Server{Catalog: cat, Tables: map[Shape]*db.ArchiveTable{}}
	var reports []BuildReport
	for _, shape := range cfg.Shapes {
		start := time.Now()
		gcfg := DefaultGenConfig(shape)
		if cfg.Seed != 0 {
			gcfg.Seed = cfg.Seed
		}
		// Every shape is built as an ARCHIVE, including jobs, which is a deliberate
		// simplification and not a claim about how a deployment stores them. The read paths
		// under test are the same for both kinds, and an archive seals segments as it fills,
		// so its scan counters are populated at any size -- a small mutable table has no
		// sealed segments yet and reports none, which would leave the readiness half of the
		// measurement blank. A client sweep pointed at a LIVE server reads that server's real
		// `jobs` table, mutable as it is; only the generated stand-in is an archive.
		arch, cerr := cat.CreateArchiveTable(string(shape), db.ArchiveConfig{
			ValueAttrs:  []string{"ClusterId", "CompletionDate"},
			ZoneAttrs:   []string{"CompletionDate"},
			SegmentSize: 1 << 20,
		})
		if cerr != nil {
			srv.Close()
			return nil, nil, fmt.Errorf("creating %s: %w", shape, cerr)
		}
		g := NewGenerator(gcfg, cfg.Rows)
		for i := 0; i < cfg.Rows; i++ {
			_, ad := g.Ad(i)
			if aerr := arch.Append(ad); aerr != nil {
				srv.Close()
				return nil, nil, fmt.Errorf("filling %s: %w", shape, aerr)
			}
			if progress != nil && cfg.Rows >= 100000 && i > 0 && i%100000 == 0 {
				_, _ = fmt.Fprintf(progress, "  %s: %d/%d rows\n", shape, i, cfg.Rows)
			}
		}
		rep := PrepareArchive(arch, DefaultPrepareOptions(shape))
		srv.Tables[shape] = arch
		reports = append(reports, BuildReport{Shape: shape, Rows: cfg.Rows, Prepare: rep, Elapsed: time.Since(start)})
	}
	return srv, reports, nil
}

// ServeAnonymous starts the dbrpc endpoint, read-only and unauthenticated, and returns the
// address it bound. The caller closes the Server to stop it.
func (s *Server) ServeAnonymous(ctx context.Context, listen string) (string, error) {
	if listen == "" {
		return "", nil
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return "", fmt.Errorf("listening on %s: %w", listen, err)
	}
	srv := dbrpc.NewServerCatalog(s.Catalog)
	// Benchmark mode aggregates query stats so a client's readiness probe gets a server-side
	// breakdown. Without it the probe reports "not measured", which is correct but useless on
	// the one server that exists to be measured.
	srv.EnableQueryStats(dbrpc.QueryStatsOptions{})
	go func() {
		for {
			c, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			go func() {
				// ReadOnly: the peer is anonymous, so it may read and snapshot and may not
				// write, retune or restructure.
				_ = srv.ServeConnOpts(dbrpc.NewStreamConn(c), dbrpc.ServeOptions{ReadOnly: true})
			}()
		}
	}()
	s.Addr = ln.Addr().String()
	prev := s.stop
	s.stop = func() {
		_ = ln.Close()
		srv.Close()
		if prev != nil {
			prev()
		}
	}
	go func() { <-ctx.Done(); _ = ln.Close() }()
	return s.Addr, nil
}
