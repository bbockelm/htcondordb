package arrowtype

import (
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/PelicanPlatform/classad/classad"
)

func TestBudgetAllowsUnderLimit(t *testing.T) {
	b := NewBudget(memory.DefaultAllocator, 1<<20)
	buf := b.Allocate(1024)
	if len(buf) != 1024 {
		t.Fatalf("allocated %d bytes, want 1024", len(buf))
	}
	if got := b.Used(); got != 1024 {
		t.Errorf("used %d, want 1024", got)
	}
	b.Free(buf)
	if got := b.Used(); got != 0 {
		t.Errorf("used %d after free, want 0", got)
	}
	if got := b.Peak(); got != 1024 {
		t.Errorf("peak %d, want 1024 -- the high-water mark is what tells an operator the limit is close", got)
	}
}

// TestBudgetRefusesOverLimit covers the refusal AND its shape. Panicking is indefensible in
// general and is the only refusal memory.Allocator permits, so the panic value has to carry
// enough for a caller's recover to report something useful.
func TestBudgetRefusesOverLimit(t *testing.T) {
	b := NewBudget(memory.DefaultAllocator, 2048)
	keep := b.Allocate(1024)
	defer b.Free(keep)

	var caught error
	func() {
		defer func() {
			if r := recover(); r != nil {
				if err, ok := r.(error); ok {
					caught = err
				} else {
					t.Errorf("panicked with %T, want an error", r)
				}
			}
		}()
		b.Allocate(4096)
	}()

	if caught == nil {
		t.Fatal("an allocation past the limit was allowed")
	}
	var over *ErrBudgetExceeded
	if !errors.As(caught, &over) {
		t.Fatalf("panicked with %v, want *ErrBudgetExceeded", caught)
	}
	if over.Limit != 2048 || over.Used != 1024 || over.Want != 4096 {
		t.Errorf("got %+v, want limit 2048 / used 1024 / want 4096", over)
	}
	// The refused allocation must not have been charged, or a caller that recovers and
	// carries on would be working against a budget that had quietly shrunk.
	if got := b.Used(); got != 1024 {
		t.Errorf("used %d after a refusal, want 1024 unchanged", got)
	}
}

func TestBudgetUnlimited(t *testing.T) {
	for _, limit := range []int64{0, -1} {
		b := NewBudget(memory.DefaultAllocator, limit)
		buf := b.Allocate(1 << 20)
		if len(buf) != 1<<20 {
			t.Errorf("limit %d: refused an allocation though unlimited", limit)
		}
		b.Free(buf)
	}
}

// TestBudgetReallocateChargesTheDelta: a builder grows its buffers, and charging the full new
// size each time would exhaust a budget that the data never actually needed.
func TestBudgetReallocateChargesTheDelta(t *testing.T) {
	b := NewBudget(memory.DefaultAllocator, 8192)
	buf := b.Allocate(1024)
	buf = b.Reallocate(2048, buf)
	if got := b.Used(); got != 2048 {
		t.Errorf("used %d after growing 1024 -> 2048, want 2048", got)
	}
	buf = b.Reallocate(512, buf)
	if got := b.Used(); got != 512 {
		t.Errorf("used %d after shrinking to 512, want 512", got)
	}
	b.Free(buf)
	if got := b.Used(); got != 0 {
		t.Errorf("used %d after free, want 0", got)
	}
}

// TestBudgetRefusesGrowthBeforeCommitting: a Reallocate that would exceed the budget has to be
// refused BEFORE the underlying allocator commits the memory, or the budget reports a limit it
// has already blown past.
func TestBudgetRefusesGrowthBeforeCommitting(t *testing.T) {
	b := NewBudget(memory.DefaultAllocator, 2048)
	buf := b.Allocate(1024)
	defer b.Free(buf)
	func() {
		defer func() { _ = recover() }()
		buf = b.Reallocate(1<<20, buf)
		t.Error("a Reallocate past the limit was allowed")
	}()
	if got := b.Used(); got > 2048 {
		t.Errorf("used %d exceeds the limit after a refused growth", got)
	}
}

// TestBudgetDoubleFreeDoesNotGoNegative: accounting that went negative would silently RAISE
// the effective limit, which is the opposite of what a budget is for.
func TestBudgetDoubleFreeDoesNotGoNegative(t *testing.T) {
	b := NewBudget(memory.DefaultAllocator, 4096)
	buf := b.Allocate(1024)
	b.Free(buf)
	b.Free(buf) // the inner allocator tolerates this; the accounting must too
	if got := b.Used(); got != 0 {
		t.Errorf("used %d after a double free, want 0", got)
	}
}

// TestBudgetStopsARealEncode is the end the budget exists for: a real encoder on a tight
// budget fails its own request rather than growing without limit.
func TestBudgetStopsARealEncode(t *testing.T) {
	decisions := []Decision{{
		Name: "Blob", Verdict: Typed,
		Field: arrow.Field{Name: "Blob", Type: arrow.BinaryTypes.String, Nullable: true},
	}}
	b := NewBudget(memory.DefaultAllocator, 4096)
	enc := NewEncoder(decisions, EncodeOptions{Alloc: b})
	defer func() {
		// Release frees through the budget too, and it must not itself panic.
		defer func() { _ = recover() }()
		enc.Release()
	}()

	big := make([]byte, 2048)
	for i := range big {
		big[i] = 'x'
	}
	ad, err := classad.ParseOld(`Blob = "` + string(big) + `"`)
	if err != nil {
		t.Fatal(err)
	}

	var over *ErrBudgetExceeded
	func() {
		defer func() {
			if r := recover(); r != nil {
				if e, ok := r.(error); ok {
					_ = errors.As(e, &over)
				}
			}
		}()
		for i := 0; i < 1000; i++ {
			_ = enc.Append(ad)
		}
	}()
	if over == nil {
		t.Fatal("a 4 KiB budget absorbed a thousand 2 KiB strings; the budget is not being applied")
	}
	t.Logf("refused at %d of %d bytes", over.Used, over.Limit)
}
