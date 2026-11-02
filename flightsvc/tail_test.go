package flightsvc

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/dbrpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/bbockelm/htcondordb/arrowtype"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// tailValues runs q against the harness client and collects the tail column of every row.
func tailValues(t *testing.T, q string) ([]string, *arrow.Schema) {
	t.Helper()
	cl, _, done := harness(t, 12)
	t.Cleanup(done)
	ctx := context.Background()

	info, err := cl.Execute(ctx, q)
	if err != nil {
		t.Fatalf("Execute(%s): %v", q, err)
	}
	rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		t.Fatalf("DoGet: %v", err)
	}
	defer rdr.Release()

	var out []string
	var schema *arrow.Schema
	for rdr.Next() {
		rec := rdr.Record()
		schema = rec.Schema()
		idx := rec.Schema().FieldIndices(arrowtype.TailColumnName)
		if len(idx) != 1 {
			t.Fatalf("%d columns named %s, want 1", len(idx), arrowtype.TailColumnName)
		}
		col, ok := rec.Column(idx[0]).(*array.String)
		if !ok {
			t.Fatalf("%s is %T, want a string array", arrowtype.TailColumnName, rec.Column(idx[0]))
		}
		for i := range int(rec.NumRows()) {
			if col.IsNull(i) {
				out = append(out, "")
				continue
			}
			out = append(out, col.Value(i))
		}
	}
	if err := rdr.Err(); err != nil {
		t.Fatalf("reading rows: %v", err)
	}
	return out, schema
}

// TestSelectAllCarriesTheTail is the schema no longer lying by omission.
//
// Requirements is in every seeded ad and gets no typed column -- its text is the only useful
// form of a policy expression. Before the tail it was invisible over this endpoint, which a
// client could not tell apart from the table not having the attribute at all.
func TestSelectAllCarriesTheTail(t *testing.T) {
	tails, schema := tailValues(t, "SELECT * FROM "+dbrpc.DefaultTable)
	if len(tails) != 12 {
		t.Fatalf("%d rows, want 12", len(tails))
	}
	if last := schema.Field(schema.NumFields() - 1).Name; last != arrowtype.TailColumnName {
		t.Errorf("the last column is %q, want %s", last, arrowtype.TailColumnName)
	}
	for i, text := range tails {
		if text == "" {
			t.Fatalf("row %d has a null tail although every ad holds Requirements", i)
		}
		ad, err := classad.Parse(text)
		if err != nil {
			t.Fatalf("row %d: the tail does not parse: %v (%s)", i, err, text)
		}
		if _, found := ad.Lookup("Requirements"); !found {
			t.Errorf("row %d: the tail has no Requirements: %s", i, text)
		}
		if _, found := ad.Lookup("Owner"); found {
			t.Errorf("row %d: the tail repeats Owner, which has its own column: %s", i, text)
		}
	}
}

// TestTailWithAProjectionReadsWholeRecords is the trap in combining the two.
//
// The tail is the complement of the typed columns, so it can only be built from a whole
// record. A pushed-down projection delivers exactly the attributes that are already columns,
// which would leave every tail empty -- a wrong answer that looks like a table with nothing
// left over, not like a failure. Asking for the tail therefore turns the projection off.
func TestTailWithAProjectionReadsWholeRecords(t *testing.T) {
	tails, schema := tailValues(t, "SELECT Owner, "+arrowtype.TailColumnName+" FROM "+dbrpc.DefaultTable)
	if schema.NumFields() != 2 {
		t.Fatalf("%d columns, want Owner and the tail", schema.NumFields())
	}
	for i, text := range tails {
		if text == "" {
			t.Fatalf("row %d has an empty tail: the projection was pushed down, so the record "+
				"arrived holding only the columns that were already typed", i)
		}
		if !strings.Contains(text, "Requirements") {
			t.Errorf("row %d: the tail does not carry Requirements: %s", i, text)
		}
		// Owner is a column here, so it is not repeated -- but ClusterId is NOT a column of
		// this projection, so it belongs in the tail. That is what makes this the
		// complement of the COLUMNS rather than of the table's schema.
		if !strings.Contains(text, "ClusterId") {
			t.Errorf("row %d: the tail does not carry ClusterId, which this query gave no column: %s", i, text)
		}
	}
}

// TestTailDoesNotCarryPrivateAttributes is the disclosure the tail opens and the one the
// column must not become.
//
// Before it, an attribute with no typed column was simply dropped by the encoder, so a private
// attribute that reached this process was discarded on the way out whatever the session was.
// The tail carries EVERYTHING a column did not take, so that accident is gone and the stripping
// has to be real. It is, and it is the loopback's: an unprivileged dbrpc session never puts the
// attribute in the row, so there is nothing here to filter -- which is exactly the property
// routing Flight through dbrpc exists to buy, and exactly the kind of property that stops
// holding the moment something downstream starts keeping what it used to throw away.
func TestTailDoesNotCarryPrivateAttributes(t *testing.T) {
	cat, cleanup := store(t)
	defer cleanup()

	for _, c := range []struct {
		name     string
		levels   []string
		wantSeen bool
	}{
		// The DAEMON case is the premise: without it a green READ case would prove only
		// that the fixture has no secret in it.
		{"daemon", []string{"READ", "WRITE", "DAEMON"}, true},
		{"read", []string{"READ"}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			sessions, err := NewSessions(cat, fixedAuth{user: "alice"},
				allowLevels("alice", c.levels...), arrowtype.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer sessions.Close()
			fsrv, err := NewSessionFlightServer(sessions, nil, Settings{})
			if err != nil {
				t.Fatal(err)
			}
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			gsrv := grpc.NewServer()
			flight.RegisterFlightServiceServer(gsrv, fsrv)
			go func() { _ = gsrv.Serve(ln) }()
			defer gsrv.Stop()
			cl, err := flightsql.NewClient(ln.Addr().String(), nil, nil,
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer cl.Close()

			ctx := context.Background()
			info, err := cl.Execute(ctx, "SELECT * FROM "+dbrpc.DefaultTable)
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
			if err != nil {
				t.Fatalf("DoGet: %v", err)
			}
			defer rdr.Release()

			// The whole batch is searched as TEXT for the secret, rather than the tail
			// column alone: a secret that arrived under some other column would be just
			// as disclosed, and a check that only looked where it was expected would
			// miss it.
			seen, rows := false, 0
			for rdr.Next() {
				rec := rdr.Record()
				rows += int(rec.NumRows())
				for c := range int(rec.NumCols()) {
					if strings.Contains(rec.Column(c).String(), "secret-") {
						seen = true
					}
				}
			}
			if err := rdr.Err(); err != nil {
				t.Fatalf("reading rows: %v", err)
			}
			if rows == 0 {
				t.Fatal("no rows came back; the case proves nothing")
			}
			if seen != c.wantSeen {
				if c.wantSeen {
					t.Fatalf("a DAEMON peer did not get ClaimId in %d rows; the fixture "+
						"has no secret to withhold, so the READ case below proves nothing", rows)
				}
				t.Fatalf("a READ peer's rows carry a private attribute's value")
			}
		})
	}
}
