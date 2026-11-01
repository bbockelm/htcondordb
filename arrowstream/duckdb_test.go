package arrowstream

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/arrowtype"
)

// Proving the output is useful to something that is not us.
//
// Every other test here reads the stream back with arrow-go -- the same library that wrote it.
// That checks the encoder against itself and says nothing about whether a real SQL engine can
// use the result, which is the entire point of emitting Arrow. This test hands the stream to
// DuckDB and runs SQL over it.
//
// It is the only test that can catch a whole class of mistake: a schema that is valid Arrow and
// useless downstream. An integer attribute typed as text still round-trips perfectly through
// arrow-go -- and `sum()` over it fails in DuckDB, which is how a reader would discover it.
//
// SKIPPING, and why it is safe here. The test skips when DuckDB is absent, which is the pattern
// that lets a test quietly never run. Two things keep it honest: the skip message says exactly
// how to make it run, and HTCONDORDB_REQUIRE_DUCKDB=1 turns every skip into a failure, so CI
// can enforce it wherever DuckDB is installed. A skip is a statement that the tool is missing,
// never that the check passed.

// duckdbPath finds the DuckDB CLI, or reports why the test cannot run.
func duckdbPath(t *testing.T) string {
	t.Helper()
	required := os.Getenv("HTCONDORDB_REQUIRE_DUCKDB") == "1"
	give := func(format string, args ...any) string {
		msg := fmt.Sprintf(format, args...)
		if required {
			t.Fatalf("HTCONDORDB_REQUIRE_DUCKDB=1 but %s", msg)
		}
		t.Skipf("skipping the DuckDB check: %s. Set HTCONDORDB_DUCKDB to a CLI binary, or "+
			"install one, and set HTCONDORDB_REQUIRE_DUCKDB=1 to make its absence a failure", msg)
		return ""
	}
	if p := os.Getenv("HTCONDORDB_DUCKDB"); p != "" {
		if _, err := os.Stat(p); err != nil {
			return give("HTCONDORDB_DUCKDB=%s is not usable: %v", p, err)
		}
		return p
	}
	p, err := exec.LookPath("duckdb")
	if err != nil {
		return give("no duckdb on PATH")
	}
	return p
}

// runDuckDB executes SQL and returns CSV output. The nanoarrow extension is a community
// extension fetched on first use, so a machine without network reports that distinctly from a
// query that genuinely failed -- the difference between "cannot run this check" and "the
// stream is wrong" matters enough to separate.
func runDuckDB(t *testing.T, bin, sql string) string {
	t.Helper()
	cmd := exec.Command(bin, "-no-agent", "-csv", "-c", "INSTALL nanoarrow FROM community; LOAD nanoarrow;\n"+sql)
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		combined := out.String() + errb.String()
		if strings.Contains(combined, "nanoarrow") &&
			(strings.Contains(combined, "network") || strings.Contains(combined, "download") ||
				strings.Contains(combined, "HTTP") || strings.Contains(combined, "Failed to download")) {
			if os.Getenv("HTCONDORDB_REQUIRE_DUCKDB") == "1" {
				t.Fatalf("HTCONDORDB_REQUIRE_DUCKDB=1 but the nanoarrow extension is unavailable: %s", combined)
			}
			t.Skipf("skipping: DuckDB cannot obtain the nanoarrow extension (needs network on first use): %s", combined)
		}
		t.Fatalf("duckdb failed: %v\n%s", err, combined)
	}
	return out.String()
}

// exportToFile writes a table as an Arrow IPC stream, through the real RPC surface.
func exportToFile(t *testing.T, path string, rows int) {
	t.Helper()
	d, err := db.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for i := 0; i < rows; i++ {
		// The shapes measured on a production AP, because they are the ones that broke:
		// an integer attribute with one stray expression, and a float attribute ClassAd
		// writes without a decimal point when the value is integral.
		src := fmt.Sprintf("Owner = \"user%d\"\nClusterId = %d\nQDate = %d\n",
			i%4, 1000+i, 1700000000+i*60)
		if i == 0 {
			src += "RequestMemory = RequestCpus * 1024\n"
		} else {
			src += "RequestMemory = 2048\n"
		}
		if i%10 == 0 {
			src += "CpusUsage = 1\n"
		} else {
			src += fmt.Sprintf("CpusUsage = %d.25\n", i%3)
		}
		src += "Requirements = TARGET.Cpus >= RequestCpus\n"
		ad, perr := classad.ParseOld(src)
		if perr != nil {
			t.Fatal(perr)
		}
		if err := d.Put(fmt.Sprintf("k%d", i), ad); err != nil {
			t.Fatal(err)
		}
	}

	s := dbrpc.NewServer(d)
	defer s.Close()
	cc, sc := net.Pipe()
	go func() { _ = s.ServeConnOpts(dbrpc.NewStreamConn(sc), dbrpc.ServeOptions{Privileged: true}) }()
	c := dbrpc.NewClient(dbrpc.NewStreamConn(cc), dbrpc.WithPeerSessionAttrs(dbrpc.ServerSessionAttrs()))
	defer c.Close()

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	res, err := Export(context.Background(), c, f, Options{
		Derive: arrowtype.Options{TimeAttrs: map[string]bool{"qdate": true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Rows != rows {
		t.Fatalf("exported %d rows, want %d", res.Rows, rows)
	}
}

// TestDuckDBReadsTheStream is the end-to-end proof: a real SQL engine, not arrow-go, computing
// over what this package emits.
func TestDuckDBReadsTheStream(t *testing.T) {
	bin := duckdbPath(t)
	path := filepath.Join(t.TempDir(), "jobs.arrows")
	const rows = 200
	exportToFile(t, path, rows)

	q := func(sql string) string {
		return strings.TrimSpace(runDuckDB(t, bin, strings.ReplaceAll(sql, "{}", path)))
	}

	// It parses at all, and the row count survives.
	got := q(`SELECT count(*) FROM read_arrow('{}');`)
	lines := strings.Split(got, "\n")
	if len(lines) < 2 || strings.TrimSpace(lines[1]) != strconv.Itoa(rows) {
		t.Fatalf("count(*) returned %q, want %d", got, rows)
	}

	// ARITHMETIC over the columns a typing mistake would have made text. This is the check
	// that could not be made from inside Go: sum() over a utf8 column fails in DuckDB and
	// round-trips perfectly through arrow-go.
	got = q(`SELECT sum(RequestMemory)::BIGINT AS total, round(avg(CpusUsage),4) AS avg_cpu
	         FROM read_arrow('{}');`)
	if !strings.Contains(got, "total,avg_cpu") {
		t.Fatalf("unexpected header in %q", got)
	}
	fields := strings.Split(strings.TrimSpace(strings.Split(got, "\n")[1]), ",")
	if len(fields) != 2 {
		t.Fatalf("expected two values, got %q", got)
	}
	total, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		t.Fatalf("sum(RequestMemory) = %q, not a number -- the column is probably text: %v", fields[0], err)
	}
	// 199 ads carry 2048; the first carries an expression evaluating to RequestCpus*1024,
	// which is undefined here and so a null that sum() skips.
	if want := int64(199 * 2048); total != want {
		t.Errorf("sum(RequestMemory) = %d, want %d", total, want)
	}
	if _, err := strconv.ParseFloat(fields[1], 64); err != nil {
		t.Errorf("avg(CpusUsage) = %q, not a number -- the int/real mix did not widen: %v", fields[1], err)
	}

	// The timestamp column must behave like a timestamp, not like an integer. A date
	// function over an int64 column is an error in DuckDB, so this is a type assertion
	// that the schema cannot fake.
	got = q(`SELECT count(DISTINCT date_trunc('hour', QDate)) AS hours FROM read_arrow('{}');`)
	hours, err := strconv.Atoi(strings.TrimSpace(strings.Split(got, "\n")[1]))
	if err != nil || hours < 2 {
		t.Errorf("date_trunc over QDate gave %q; the column is not a usable timestamp", got)
	}

	// A predicate on the string column must be case-SENSITIVE in DuckDB, as it is in the
	// endpoint's own lowering -- the two have to agree or a user migrating between them
	// gets different answers.
	got = q(`SELECT count(*) FROM read_arrow('{}') WHERE Owner = 'user1';`)
	if n := strings.TrimSpace(strings.Split(got, "\n")[1]); n != "50" {
		t.Errorf("Owner = 'user1' matched %s rows, want 50", n)
	}
	got = q(`SELECT count(*) FROM read_arrow('{}') WHERE Owner = 'USER1';`)
	if n := strings.TrimSpace(strings.Split(got, "\n")[1]); n != "0" {
		t.Errorf("Owner = 'USER1' matched %s rows, want 0", n)
	}

	// Requirements got no typed column, so DuckDB must not see one. A client that needs it
	// reads the tail; a column of meaningless booleans would be worse than its absence.
	got = q(`SELECT count(*) FROM (DESCRIBE SELECT * FROM read_arrow('{}')) WHERE column_name = 'Requirements';`)
	if n := strings.TrimSpace(strings.Split(got, "\n")[1]); n != "0" {
		t.Errorf("Requirements appears as a column (%s); it is a policy expression with no scalar form", n)
	}
}

// TestDuckDBJoinsAgainstLocalData is the use case the whole effort is for: pool data joined to
// something the user already has, in one query, without an export step.
func TestDuckDBJoinsAgainstLocalData(t *testing.T) {
	bin := duckdbPath(t)
	path := filepath.Join(t.TempDir(), "jobs.arrows")
	exportToFile(t, path, 200)

	sql := strings.ReplaceAll(`
		CREATE TABLE quota(owner VARCHAR, job_limit INT);
		INSERT INTO quota VALUES ('user0', 10), ('user1', 100);
		SELECT q.owner, count(*) AS jobs, count(*) > q.job_limit AS over
		FROM read_arrow('{}') h JOIN quota q ON h.Owner = q.owner
		GROUP BY q.owner, q.job_limit ORDER BY q.owner;`, "{}", path)
	out := strings.TrimSpace(runDuckDB(t, bin, sql))

	// 200 rows over four owners is 50 each: user0 is over a limit of 10, user1 is not
	// over 100. The join is what is being tested; the arithmetic just makes it checkable.
	want := "owner,jobs,over\nuser0,50,true\nuser1,50,false"
	if out != want {
		t.Errorf("join produced:\n%s\nwant:\n%s", out, want)
	}
}
