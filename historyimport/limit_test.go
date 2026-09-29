package historyimport

import (
	"testing"

	htcondor "github.com/bbockelm/golang-htcondor"
)

// TestHistoryQueryIsUncapped pins the wire-level meaning of "no cap". The history
// client treats a zero Limit as "unset" and substitutes 50, and a zero ScanLimit as
// 10000; because the scan is newest-first and the cursor advances to the newest
// record seen, a truncated cycle does not merely defer the rest -- it skips it
// permanently.
func TestHistoryQueryIsUncapped(t *testing.T) {
	for _, tc := range []struct {
		name      string
		in        int
		wantLimit int
	}{
		{"no cap configured", 0, -1},
		{"negative is already unlimited", -1, -1},
		{"an explicit cap is passed through", 500, 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := historyOptions(htcondor.HistorySourceJobHistory, "", tc.in)
			if opts.Limit != tc.wantLimit {
				t.Errorf("Limit = %d, want %d", opts.Limit, tc.wantLimit)
			}
			if opts.ScanLimit != -1 {
				t.Errorf("ScanLimit = %d, want -1 (0 is read as the client's default of 10000)", opts.ScanLimit)
			}
			if effective := opts.ApplyDefaults(); tc.wantLimit == -1 && !effective.IsUnlimited() {
				t.Errorf("after ApplyDefaults the query is capped at %d", effective.Limit)
			}
		})
	}
}
