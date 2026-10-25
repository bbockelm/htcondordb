package repl

import (
	"reflect"
	"testing"
)

// TestUnionNames: the daemon's table listing carries append-only archives from the release
// that merged the two namespaces, and did not before it. Both must give each name once.
func TestUnionNames(t *testing.T) {
	for _, tc := range []struct {
		name     string
		tables   []string
		archives []string
		want     []string
	}{
		{
			name:     "daemon lists archives itself",
			tables:   []string{"history", "jobs"},
			archives: []string{"history"},
			want:     []string{"history", "jobs"},
		},
		{
			name:     "older daemon lists only mutable tables",
			tables:   []string{"jobs"},
			archives: []string{"history"},
			want:     []string{"jobs", "history"},
		},
		{
			name:     "no archives",
			tables:   []string{"jobs"},
			archives: nil,
			want:     []string{"jobs"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := unionNames(tc.tables, tc.archives); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("unionNames = %v, want %v", got, tc.want)
			}
		})
	}
}
