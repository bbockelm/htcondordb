package bench

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/PelicanPlatform/classad/dbrpc"
)

// Build must refuse a foreign directory BEFORE opening anything. The ordering is the point: a
// guard that ran after the store was opened would have already created files in someone's
// directory by the time it objected.
func TestBuildRefusesForeignDirectoryBeforeOpening(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "job_queue.log"), []byte("real"), 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	srv, _, berr := Build(ServeConfig{Dir: dir, Rows: 100}, nil)
	if berr == nil {
		srv.Close()
		t.Fatal("Build accepted a directory holding foreign data")
	}
	if !errors.Is(berr, ErrNotScratch) {
		t.Fatalf("error does not wrap ErrNotScratch: %v", berr)
	}
	after, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		var names []string
		for _, e := range after {
			names = append(names, e.Name())
		}
		t.Fatalf("Build wrote into a directory it refused: %v", names)
	}
}

// The built server serves every shape, anonymously and read-only.
func TestBuiltServerServesAnonymousReads(t *testing.T) {
	srv, reports, err := Build(ServeConfig{Dir: filepath.Join(t.TempDir(), "scratch"), Rows: 600}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	if len(reports) != 3 {
		t.Fatalf("%d tables built, want 3", len(reports))
	}
	for _, r := range reports {
		if !r.Prepare.SchemaScanOn {
			t.Errorf("%s: columnar accelerator did not build: %v", r.Shape, r.Prepare.Notes)
		}
	}
	addr, err := srv.ServeAnonymous(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dial := dialAddr(t, addr)
	// Driven through Run so this exercises every transport the tool offers, against each table
	// a deployment serves -- including the whole-ad form, which an archive refuses through the
	// op its name suggests and serves through the projection op with no attributes.
	for _, shape := range []Shape{ShapeJobs, ShapeHistory, ShapeEpochHistory} {
		rep, rerr := Run(context.Background(), dial, Spec{
			Table: string(shape), Constraint: "true",
			Projection: []string{"ClusterId", "Owner", "JobStatus"},
		})
		if rerr != nil {
			t.Fatalf("%s: %v", shape, rerr)
		}
		for _, smp := range rep.Samples {
			if smp.Err != nil {
				t.Errorf("%s/%s: anonymous read failed: %v", shape, smp.Transport, smp.Err)
				continue
			}
			if smp.Rows != 600 {
				t.Errorf("%s/%s: %d rows, want 600", shape, smp.Transport, smp.Rows)
			}
			if smp.Bytes <= 0 {
				t.Errorf("%s/%s: no bytes measured", shape, smp.Transport)
			}
		}
	}
}

// Anonymous means READ-only. A benchmark server holds invented data, but an anonymous writer is
// a different thing from an anonymous reader and only the second was asked for.
func TestAnonymousPeerCannotWrite(t *testing.T) {
	srv, _, err := Build(ServeConfig{Dir: filepath.Join(t.TempDir(), "scratch"),
		Rows: 150, Shapes: []Shape{ShapeHistory}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	addr, err := srv.ServeAnonymous(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cn, derr := dialAddr(t, addr)(context.Background())
	if derr != nil {
		t.Fatal(derr)
	}
	defer cn.Close()
	tx, terr := cn.Client.BeginTable(context.Background(), "history")
	if terr != nil {
		return // refused at Begin is a refusal too
	}
	if aerr := tx.SetAttribute(context.Background(), "1.0", "Owner", `"mallory"`); aerr == nil {
		if cerr := tx.Commit(context.Background()); cerr == nil {
			t.Fatal("an anonymous peer wrote to the benchmark store")
		}
	}
}

// A built dataset is reproducible across runs: the same seed and row count give the same bytes,
// so two machines are compared on identical data.
func TestBuiltDatasetIsReproducible(t *testing.T) {
	read := func() string {
		srv, _, err := Build(ServeConfig{Dir: filepath.Join(t.TempDir(), "scratch"),
			Rows: 300, Shapes: []Shape{ShapeHistory}, Seed: 7}, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer srv.Close()
		seq, qerr := srv.Tables[ShapeHistory].Query("true")
		if qerr != nil {
			t.Fatal(qerr)
		}
		var out string
		n := 0
		for ad := range seq {
			out += ad.String()
			n++
			if n == 20 {
				break
			}
		}
		return out
	}
	if a, b := read(), read(); a != b {
		t.Fatal("two builds with the same seed produced different data")
	}
}

func dialAddr(t *testing.T, addr string) Dialer {
	t.Helper()
	return func(context.Context) (*Conn, error) {
		c, err := netDial(addr)
		if err != nil {
			return nil, err
		}
		counted, bytes := NewCountingConn(c)
		cl := dbrpc.NewClient(dbrpc.NewStreamConn(counted))
		return &Conn{Client: cl, Close: func() { _ = cl.Close() }, Bytes: bytes}, nil
	}
}

func netDial(addr string) (net.Conn, error) { return net.Dial("tcp", addr) }
