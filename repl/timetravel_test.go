package repl

import (
	"fmt"
	"testing"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// TestParseAsOf covers the SQL temporal clause in both spellings.
func TestParseAsOf(t *testing.T) {
	for _, q := range []string{
		`SELECT count(*) FROM jobs FOR SYSTEM_TIME AS OF '2026-07-19T10:00:00Z' WHERE JobStatus == 2`,
		`SELECT * FROM jobs AS OF '-1h'`,
	} {
		st, err := Parse(q)
		if err != nil {
			t.Fatalf("Parse(%q): %v", q, err)
		}
		if st.AsOf == "" {
			t.Errorf("Parse(%q): AsOf not set", q)
		}
	}
	// No temporal clause -> AsOf empty.
	st, _ := Parse(`SELECT * FROM jobs WHERE JobStatus == 2`)
	if st.AsOf != "" {
		t.Errorf("AsOf = %q for a plain SELECT, want empty", st.AsOf)
	}
}

// TestExecTimeTravel drives the full CLI path over dbrpc: enable via the admin action,
// write two versions, and confirm SELECT ... AS OF (rows) and count(*) ... AS OF
// resolve the historical state.
func TestExecTimeTravel(t *testing.T) {
	e, cat, cleanup := newPrivCatalogExec(t)
	defer cleanup()
	d, _ := cat.Table(dbrpc.DefaultTable)

	// Enable time travel via the same admin action the .timetravel meta-command uses.
	if _, err := e.Admin(dbrpc.DefaultTable, "timetravel.enable", "3600", "1"); err != nil {
		t.Fatalf("timetravel.enable: %v", err)
	}

	put := func(status int) {
		tx := d.Begin()
		ad, err := classad.Parse(fmt.Sprintf(`[Key="k"; JobStatus=%d]`, status))
		if err != nil {
			t.Fatal(err)
		}
		tx.NewClassAd("k", ad)
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	put(1)
	time.Sleep(1100 * time.Millisecond) // cross the 1s checkpoint boundary
	mid := time.Now()
	time.Sleep(1100 * time.Millisecond)
	put(2)

	midTS := mid.Format(time.RFC3339)

	// SELECT count(*) AS OF mid: JobStatus was 1 then.
	res, err := e.ExecString(fmt.Sprintf(`SELECT count(*) FROM %s AS OF '%s' WHERE JobStatus == 1`, dbrpc.DefaultTable, midTS))
	if err != nil {
		t.Fatalf("count(*) AS OF: %v", err)
	}
	if len(res.Rows) != 1 || res.Rows[0][0] != "1" {
		t.Errorf("count(JobStatus==1) AS OF mid = %v, want [[1]]", res.Rows)
	}

	// count(*) with JobStatus==2 AS OF mid is 0 (not written yet).
	res, _ = e.ExecString(fmt.Sprintf(`SELECT count(*) FROM %s AS OF '%s' WHERE JobStatus == 2`, dbrpc.DefaultTable, midTS))
	if len(res.Rows) != 1 || res.Rows[0][0] != "0" {
		t.Errorf("count(JobStatus==2) AS OF mid = %v, want [[0]]", res.Rows)
	}

	// Plain SELECT * AS OF now: JobStatus is 2. Sleep past the checkpoint boundary so
	// the second-granularity RFC3339 "now" is at/after put(2)'s checkpoint.
	time.Sleep(1100 * time.Millisecond)
	res, err = e.ExecString(fmt.Sprintf(`SELECT * FROM %s AS OF '%s'`, dbrpc.DefaultTable, time.Now().Format(time.RFC3339)))
	if err != nil {
		t.Fatalf("SELECT * AS OF now: %v", err)
	}
	if len(res.Ads) != 1 {
		t.Fatalf("SELECT * AS OF now returned %d ads, want 1", len(res.Ads))
	}
	if v, _ := res.Ads[0].EvaluateAttrInt("JobStatus"); v != 2 {
		t.Errorf("JobStatus AS OF now = %d, want 2", v)
	}
}
