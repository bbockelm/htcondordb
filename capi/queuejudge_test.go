package main

import (
	"testing"

	"github.com/PelicanPlatform/classad/classad"
)

// adFrom builds a sync-health ad from the attributes a test cares about.
func adFrom(t *testing.T, attrs map[string]any) *classad.ClassAd {
	t.Helper()
	ad := classad.New()
	for k, v := range attrs {
		switch x := v.(type) {
		case bool:
			ad.InsertAttrBool(k, x)
		case int64:
			ad.InsertAttr(k, x)
		case string:
			ad.InsertAttrString(k, x)
		default:
			t.Fatalf("unsupported attribute type for %s", k)
		}
	}
	return ad
}

func TestJudge(t *testing.T) {
	// The client is built per case rather than stored in the table: queueClient
	// carries a mutex, and a table of them cannot be ranged over.
	cases := []struct {
		name       string
		maxLagSec  int64
		wantSource string
		attrs      map[string]any
		want       int32
	}{
		{
			name:      "caught up and recent is ready",
			maxLagSec: 30,
			attrs:     map[string]any{"JobQueueCaughtUp": true, "JobQueueSecondsSinceSync": int64(3)},
			want:      hcdbQueueReady,
		},
		{
			// A history-only mirror advertises nothing about the live queue, which
			// parses to the same false/0 as "caught up with no lag". Telling the two
			// apart is the whole reason presence is checked rather than value.
			name:      "a mirror that says nothing about the job queue is stale",
			maxLagSec: 30,
			attrs:     map[string]any{"HistoryCaughtUp": true},
			want:      hcdbQueueStale,
		},
		{
			name:      "behind the log is stale",
			maxLagSec: 30,
			attrs:     map[string]any{"JobQueueCaughtUp": false, "JobQueueLagBytes": int64(4096)},
			want:      hcdbQueueStale,
		},
		{
			name:      "a stale last pass is stale",
			maxLagSec: 30,
			attrs:     map[string]any{"JobQueueCaughtUp": true, "JobQueueSecondsSinceSync": int64(31)},
			want:      hcdbQueueStale,
		},
		{
			// Without the time bound only the end-of-file check applies, which is
			// what ALWAYS asks for.
			name:      "no time bound accepts an old pass",
			maxLagSec: 0,
			attrs:     map[string]any{"JobQueueCaughtUp": true, "JobQueueSecondsSinceSync": int64(3600)},
			want:      hcdbQueueReady,
		},
		{
			name:      "a mirror that cannot say when it last read is stale under a time bound",
			maxLagSec: 30,
			attrs:     map[string]any{"JobQueueCaughtUp": true},
			want:      hcdbQueueStale,
		},
		{
			// The check that no freshness measure can make: a current mirror of
			// SOMEBODY ELSE'S schedd.
			name:      "a different job queue is stale",
			maxLagSec: 30, wantSource: "/var/lib/condor/spool/job_queue.log",
			attrs: map[string]any{"JobQueueCaughtUp": true, "JobQueueSecondsSinceSync": int64(1), "JobQueueSource": "/other/spool/job_queue.log"},
			want:  hcdbQueueStale,
		},
		{
			name:      "our own job queue is ready",
			maxLagSec: 30, wantSource: "/var/lib/condor/spool/job_queue.log",
			attrs: map[string]any{"JobQueueCaughtUp": true, "JobQueueSecondsSinceSync": int64(1), "JobQueueSource": "/var/lib/condor/spool/job_queue.log"},
			want:  hcdbQueueReady,
		},
		{
			// A mirror too old to advertise its source must not be refused outright;
			// the freshness checks still stand.
			name:      "a mirror that does not name its source is judged on freshness alone",
			maxLagSec: 30, wantSource: "/var/lib/condor/spool/job_queue.log",
			attrs: map[string]any{"JobQueueCaughtUp": true, "JobQueueSecondsSinceSync": int64(1)},
			want:  hcdbQueueReady,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := queueClient{maxLagSec: tc.maxLagSec, wantSource: tc.wantSource}
			got, why := q.judge(adFrom(t, tc.attrs))
			if got != tc.want {
				t.Fatalf("judge = %d (%s), want %d", got, why, tc.want)
			}
			if why == "" {
				t.Errorf("judge gave no reason")
			}
		})
	}
}
