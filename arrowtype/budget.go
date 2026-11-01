package arrowtype

import (
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow/memory"
)

// A byte budget for Arrow allocation, because the default allocator has none.
//
// memory.DefaultAllocator grows until the process cannot. In a separate gateway that is a
// crashed gateway; in process it is the daemon, and a daemon that dies taking the job queue's
// reader with it is an outage caused by one client asking for a wide table. The point of a
// budget is that the QUERY fails instead.
//
// WHY IT PANICS, which is otherwise indefensible. memory.Allocator is
// `Allocate(size int) []byte` -- no error return, and returning nil makes a builder write into
// a short slice. Panicking is the only refusal the interface permits, so the budget panics
// with a typed error and the caller is expected to have a recover that turns it into a failed
// request. flightsvc's DoGet handler has one; anything else using a budget needs one too, and
// that obligation is the price of the interface rather than a choice made here.

// ErrBudgetExceeded is the value a Budget panics with.
type ErrBudgetExceeded struct {
	Limit, Used, Want int64
}

func (e *ErrBudgetExceeded) Error() string {
	return fmt.Sprintf("arrow allocation budget exceeded: %d bytes in use, %d more requested, limit %d",
		e.Used, e.Want, e.Limit)
}

// Budget wraps an allocator and refuses to exceed a byte limit. It is safe for concurrent use,
// because Arrow builders for different columns of one batch may allocate from it at once.
type Budget struct {
	inner memory.Allocator
	limit int64

	mu   sync.Mutex
	used int64
	peak int64
	// sizes records each live buffer's accounted size, keyed by the address of its first
	// byte. Free is handed the slice and not the size it was allocated with, and a
	// Reallocate may hand back a slice whose capacity differs from what was asked for, so
	// the only reliable accounting is what this charged at the time.
	sizes map[*byte]int64
}

// NewBudget wraps inner with a limit in bytes. A limit of zero or less means unlimited, which
// is the default allocator's behaviour and is what a caller that has not configured a budget
// gets -- explicit rather than implied.
func NewBudget(inner memory.Allocator, limit int64) *Budget {
	if inner == nil {
		inner = memory.DefaultAllocator
	}
	return &Budget{inner: inner, limit: limit, sizes: map[*byte]int64{}}
}

// Limit is the configured cap; 0 means unlimited.
func (b *Budget) Limit() int64 { return b.limit }

// Used is the bytes currently charged.
func (b *Budget) Used() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}

// Peak is the high-water mark, which is the number worth reporting: a stream that finished
// inside its budget still says how close it came, and that is what tells an operator whether
// the limit is about to start refusing real work.
func (b *Budget) Peak() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.peak
}

func (b *Budget) charge(want int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.limit > 0 && b.used+want > b.limit {
		panic(&ErrBudgetExceeded{Limit: b.limit, Used: b.used, Want: want})
	}
	b.used += want
	if b.used > b.peak {
		b.peak = b.used
	}
}

func (b *Budget) record(buf []byte, size int64) {
	if len(buf) == 0 {
		// A zero-length allocation has no address to key on. It is charged nothing, so
		// there is nothing to forget either.
		return
	}
	b.mu.Lock()
	b.sizes[&buf[0]] = size
	b.mu.Unlock()
}

func (b *Budget) forget(buf []byte) int64 {
	if len(buf) == 0 {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	key := &buf[0]
	size, ok := b.sizes[key]
	if !ok {
		// A buffer this budget never charged -- freed twice, or allocated elsewhere and
		// freed here. Returning 0 keeps the accounting from going negative, which would
		// silently raise the effective limit rather than report anything.
		return 0
	}
	delete(b.sizes, key)
	b.used -= size
	return size
}

func (b *Budget) Allocate(size int) []byte {
	b.charge(int64(size))
	buf := b.inner.Allocate(size)
	b.record(buf, int64(size))
	return buf
}

func (b *Budget) Reallocate(size int, buf []byte) []byte {
	// Charge the DELTA, and charge it before reallocating: a growth that would exceed the
	// budget must be refused before the underlying allocator commits the memory.
	old := b.accounted(buf)
	if delta := int64(size) - old; delta > 0 {
		b.charge(delta)
	} else {
		b.mu.Lock()
		b.used += delta
		b.mu.Unlock()
	}
	out := b.inner.Reallocate(size, buf)
	b.mu.Lock()
	if len(buf) > 0 {
		delete(b.sizes, &buf[0])
	}
	b.mu.Unlock()
	b.record(out, int64(size))
	return out
}

func (b *Budget) Free(buf []byte) {
	b.forget(buf)
	b.inner.Free(buf)
}

func (b *Budget) accounted(buf []byte) int64 {
	if len(buf) == 0 {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sizes[&buf[0]]
}
