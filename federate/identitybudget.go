package federate

import "sync"

// identityBudget bounds how many identity entries the archive sinks of one hub hold AT ONCE,
// across every (schedd, table) sink, rather than giving each sink a ceiling of its own.
//
// The bound used to be per sink: 64 MiB each, sized so that the sinks of eight APs times three
// archive tables replaying at the same time could not exhaust a 16 GiB GOMEMLIMIT. The arithmetic
// was right and the result was useless. One AP's job_metrics archive holds well over the 838,860
// identities 64 MiB buys, so the load was abandoned EVERY time and that sink fell back to one
// exact query per record -- measured at 0.09 records/s against a 13.5 GB archive, slow enough that
// a replay cannot finish before the next one starts. The sink applied zero records in seven and a
// half hours.
//
// Replays are rare, and rarely simultaneous. Pooling the same memory lets a lone replay take all
// of it and finish, while concurrent replays still share one fixed ceiling -- each reserving in
// chunks as its set grows, so they interleave rather than the first one taking everything.
type identityBudget struct {
	mu    sync.Mutex
	limit int
	used  int
}

// defaultIdentityBudget is the pooled ceiling, in identities. At bytesPerIdentity this is 1 GiB
// against a 16 GiB GOMEMLIMIT: enough for any one AP's archive seen in production, and still a
// sixteenth of the daemon's memory if every sink replays at once.
const defaultIdentityBudget = (1 << 30) / bytesPerIdentity

// identityReserveChunk is how many identities a sink takes from the budget at a time. Reserving in
// chunks is what makes concurrent loads share: each takes a slice as it grows instead of claiming
// a worst case up front.
const identityReserveChunk = 1 << 16

func newIdentityBudget(limit int) *identityBudget { return &identityBudget{limit: limit} }

// reserve takes n more identities, reporting whether the budget had room. A refusal leaves the
// budget untouched: the caller abandons its set and releases what it already holds.
func (b *identityBudget) reserve(n int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.used+n > b.limit {
		return false
	}
	b.used += n
	return true
}

// release returns n identities to the budget.
func (b *identityBudget) release(n int) {
	if n <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.used -= n; b.used < 0 {
		b.used = 0
	}
}
