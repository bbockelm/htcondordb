package bench

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

// serveBoth builds a scratch server with both endpoints and returns a dialer that opens a
// connection speaking both.
func serveBoth(t *testing.T, rows int) (Dialer, *Server) {
	t.Helper()
	srv, _, err := Build(ServeConfig{Dir: filepath.Join(t.TempDir(), "scratch"),
		Rows: rows, Shapes: []Shape{ShapeHistory}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)
	dbAddr, err := srv.ServeAnonymous(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	flAddr, err := srv.ServeFlight(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return func(ctx context.Context) (*Conn, error) {
		dbc, derr := dialAddr(t, dbAddr)(ctx)
		if derr != nil {
			return nil, derr
		}
		fc, ferr := DialFlight(flAddr)
		if ferr != nil {
			dbc.Close()
			return nil, ferr
		}
		return &Conn{
			Client: dbc.Client,
			Flight: fc.Flight,
			Close:  func() { fc.Close(); dbc.Close() },
			// Both sockets' bytes, which is what this connection received.
			Bytes: func() int64 { return dbc.Bytes() + fc.Bytes() },
		}, nil
	}, srv
}

// The Arrow arm runs beside the ClassAd ones and is counted the same way.
func TestArrowTransportMeasured(t *testing.T) {
	dial, _ := serveBoth(t, 3000)
	rep, err := Run(context.Background(), dial, Spec{
		Table: "history", Constraint: "true",
		Projection: []string{"ClusterId", "Owner", "JobStatus"},
		Transports: []Transport{Wire, Values, Arrow},
	})
	if err != nil {
		t.Fatal(err)
	}
	var arrow Sample
	for _, s := range rep.Samples {
		if s.Err != nil {
			t.Fatalf("%s: %v (%s)", s.Transport, s.Err, s.Note)
		}
		if s.Rows != 3000 {
			t.Errorf("%s: %d rows, want 3000", s.Transport, s.Rows)
		}
		if s.Transport == Arrow {
			arrow = s
		}
	}
	if arrow.Bytes <= 0 {
		t.Fatal("the Arrow arm reported no bytes; the gRPC socket is not being counted")
	}
	if !arrow.Transport.Evaluated() {
		t.Error("Arrow is not classified as an evaluated form")
	}
	t.Logf("arrow: %d rows, %.1f MB/s, %.1f B/row", arrow.Rows, arrow.MBPerSec(), arrow.BytesPerRow())
}

// THE ONE THAT KEEPS THE COMPARISON HONEST: a ClassAd constraint the Arrow arm cannot express
// must make that arm DECLINE, not run unfiltered.
//
// An unfiltered Arrow read beside a filtered ClassAd one is a different question, and it would
// look fast for a reason that has nothing to do with the transport.
func TestArrowDeclinesAnUntranslatableConstraint(t *testing.T) {
	spec := Spec{Table: "history", Constraint: "JobStatus == 3",
		Projection: []string{"ClusterId"}}
	if _, err := arrowQuery(spec, 0); err == nil {
		t.Fatal("the Arrow arm accepted a ClassAd constraint it cannot express")
	} else if !strings.Contains(err.Error(), "SQLWhere") {
		t.Errorf("the refusal does not say how to fix it: %v", err)
	}
	// With the caller's SQL it runs, and the filter is in the query.
	spec.SQLWhere = "JobStatus = 3"
	q, err := arrowQuery(spec, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(q, "WHERE JobStatus = 3") {
		t.Errorf("query %q does not carry the caller's filter", q)
	}
	// A trivially-true constraint needs no translation.
	spec.Constraint, spec.SQLWhere = "true", ""
	if q, err = arrowQuery(spec, 10); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(q, "WHERE") || !strings.Contains(q, "LIMIT 10") {
		t.Errorf("query %q should have no WHERE and a LIMIT", q)
	}
}

// A declining arm must report itself rather than returning a rate of zero that reads as slow.
func TestArrowDeclineSurfacesInTheReport(t *testing.T) {
	dial, _ := serveBoth(t, 500)
	rep, err := Run(context.Background(), dial, Spec{
		Table: "history", Constraint: "JobStatus == 3",
		Projection: []string{"ClusterId"},
		Transports: []Transport{Arrow},
	})
	if err != nil {
		t.Fatal(err)
	}
	s := rep.Samples[0]
	if s.Err == nil {
		t.Fatalf("the Arrow arm ran with an untranslatable constraint and reported %.1f MB/s", s.MBPerSec())
	}
	if !strings.Contains(s.Note, "SQLWhere") {
		t.Errorf("the note does not explain the decline: %q", s.Note)
	}
}
