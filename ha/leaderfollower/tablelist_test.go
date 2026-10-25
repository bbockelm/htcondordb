package leaderfollower

import (
	"reflect"
	"testing"
)

// TestWithoutArchives: replication applies watch events to a table it creates with
// CreateTable, which an archive cannot be. A daemon that lists archives alongside tables
// must not therefore get them replicated -- on a follower holding the same archive
// CreateTable fails and loops the session, and on one that does not it creates a mutable
// table of that name instead.
func TestWithoutArchives(t *testing.T) {
	for _, tc := range []struct {
		name     string
		tables   []string
		archives []string
		want     []string
	}{
		{
			name:     "daemon lists archives among the tables",
			tables:   []string{"history", "jobs", "metrics"},
			archives: []string{"history"},
			want:     []string{"jobs", "metrics"},
		},
		{
			name:     "older daemon lists only mutable tables",
			tables:   []string{"jobs"},
			archives: []string{"history"},
			want:     []string{"jobs"},
		},
		{
			name:     "no archives at all",
			tables:   []string{"jobs"},
			archives: nil,
			want:     []string{"jobs"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := withoutArchives(tc.tables, tc.archives); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("withoutArchives = %v, want %v", got, tc.want)
			}
		})
	}
}
