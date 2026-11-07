package bench

import (
	"math"
	"testing"
	"time"
)

// attrRange summarises one numeric attribute over a whole generated dataset.
type attrRange struct {
	min, max float64
	sum      float64
	n        int
	distinct map[string]bool
}

// mean is the statistic the invariance check rests on. Observed MIN and MAX of a wide
// distribution converge with sample size -- comparing them between two sizes tests how many draws
// were taken, not whether the distribution moved. The mean is stable under sampling and shifts
// decisively when the distribution really does depend on the row count: a value defined as
// `base + i` has a mean that grows with N, where a fixed uniform's does not.
func (r *attrRange) mean() float64 {
	if r.n == 0 {
		return math.NaN()
	}
	return r.sum / float64(r.n)
}

// summarise generates rows ads and reports, per attribute, the numeric range and the distinct
// string values. MemoryUsage is EVALUATED rather than read, because it is an expression -- which
// is exactly how its dependence on the row count hid in the fixture this generator replaces.
func summarise(t *testing.T, cfg GenConfig, rows int, attrs []string) map[string]*attrRange {
	t.Helper()
	g := NewGenerator(cfg, rows)
	out := map[string]*attrRange{}
	for _, a := range attrs {
		out[a] = &attrRange{min: math.Inf(1), max: math.Inf(-1), distinct: map[string]bool{}}
	}
	for i := 0; i < rows; i++ {
		_, ad := g.Ad(i)
		for _, a := range attrs {
			v := ad.EvaluateAttr(a)
			if n, err := v.IntValue(); err == nil {
				r := out[a]
				r.min = math.Min(r.min, float64(n))
				r.max = math.Max(r.max, float64(n))
				r.sum += float64(n)
				r.n++
				continue
			}
			if s, err := v.StringValue(); err == nil {
				out[a].distinct[s] = true
			}
		}
	}
	return out
}

// The property the generator exists for: doubling the row count must not move any value
// distribution. A generator that fails this varies two things at once, and the second one reads
// as a property of scale.
//
// MemoryUsage is in the list deliberately. It is an expression over ResidentSetSize, and in the
// fixture this replaces ResidentSetSize was `90000 + i` -- so MemoryUsage's range grew with the
// row count, changing the column's width and compressibility between sizes of the same sweep.
func TestDatasetIsScaleInvariant(t *testing.T) {
	cfg := DefaultGenConfig(ShapeHistory)
	attrs := []string{
		"RequestCpus", "RequestMemory", "RequestDisk", "ResidentSetSize", "ImageSize",
		"JobStatus", "MemoryUsage", "Owner", "ProjectName", "Cmd",
	}
	small := summarise(t, cfg, 5000, attrs)
	large := summarise(t, cfg, 50000, attrs)

	for _, a := range attrs {
		s, l := small[a], large[a]
		if len(s.distinct) > 0 || len(l.distinct) > 0 {
			// A string attribute: its CARDINALITY is set by the config, so it must not grow
			// with rows. (The smaller sample may not have drawn every value, so the small set
			// must be a subset of the large one and the large one must respect the cap.)
			if len(l.distinct) > len(s.distinct) && len(s.distinct) < cardinalityCap(cfg, a) {
				continue // the small sample simply had not drawn them all
			}
			if len(l.distinct) > cardinalityCap(cfg, a) {
				t.Errorf("%s: %d distinct values at 50k, config caps it at %d",
					a, len(l.distinct), cardinalityCap(cfg, a))
			}
			continue
		}
		if math.IsInf(s.min, 1) || math.IsInf(l.min, 1) {
			t.Errorf("%s: never produced a numeric value; the test is not measuring it", a)
			continue
		}
		// The mean must not move: that is what a scale-dependent value changes, and what
		// sampling does not.
		sm, lm := s.mean(), l.mean()
		if rel := math.Abs(sm-lm) / math.Max(math.Abs(lm), 1); rel > 0.02 {
			t.Errorf("%s: mean %g at 5k but %g at 50k (%.1f%%); this attribute's distribution depends on the row count",
				a, sm, lm, rel*100)
		}
		// And the upper bound must not grow with the row count, which is the shape a value
		// defined from the row index has even when its mean shift is small.
		if l.max > s.max*1.05+1 {
			t.Errorf("%s: max %g at 5k grew to %g at 50k; the attribute's range tracks the row count",
				a, s.max, l.max)
		}
	}
}

func cardinalityCap(cfg GenConfig, attr string) int {
	switch attr {
	case "Owner":
		return cfg.Owners
	case "ProjectName":
		return cfg.Projects
	case "Cmd":
		return cfg.Commands
	}
	return math.MaxInt32
}

// Timestamps must span the CONFIGURED window whatever the row count. One row per second makes the
// window a function of N, which makes every zone map differently selective between sizes of one
// sweep -- the same mistake as a value range that grows, in the attribute archives prune on.
func TestDatasetTimestampsSpanTheConfiguredWindow(t *testing.T) {
	cfg := DefaultGenConfig(ShapeHistory)
	for _, rows := range []int{1000, 100000} {
		g := NewGenerator(cfg, rows)
		var lo, hi int64 = math.MaxInt64, math.MinInt64
		for i := 0; i < rows; i++ {
			_, ad := g.Ad(i)
			v, err := ad.EvaluateAttr("CompletionDate").IntValue()
			if err != nil {
				t.Fatalf("CompletionDate: %v", err)
			}
			if v < lo {
				lo = v
			}
			if v > hi {
				hi = v
			}
		}
		span := time.Duration(hi-lo) * time.Second
		// Within one row's worth of the configured window.
		slack := cfg.Window / time.Duration(rows)
		if span < cfg.Window-slack-time.Second || span > cfg.Window+time.Second {
			t.Errorf("rows=%d: timestamps span %v, want the configured %v", rows, span, cfg.Window)
		}
	}
}

// The sparse-attribute rate is a lever on how much of a projected read escapes the columnar path,
// so it must be what the config says and not drift with scale.
func TestSparseAttributeRateFollowsTheConfig(t *testing.T) {
	cfg := DefaultGenConfig(ShapeHistory)
	cfg.SparseFraction = 0.10
	for _, rows := range []int{20000, 200000} {
		g := NewGenerator(cfg, rows)
		present := 0
		for i := 0; i < rows; i++ {
			_, ad := g.Ad(i)
			if _, ok := ad.Lookup("HoldReasonCode"); ok {
				present++
			}
		}
		got := float64(present) / float64(rows)
		if math.Abs(got-cfg.SparseFraction) > 0.01 {
			t.Errorf("rows=%d: sparse attribute on %.3f of ads, want %.3f", rows, got, cfg.SparseFraction)
		}
	}
}

// A dataset must be reproducible: the same seed and config give identical ads, or a comparison
// between two machines is a comparison of two datasets.
func TestDatasetIsReproducible(t *testing.T) {
	cfg := DefaultGenConfig(ShapeJobs)
	a := NewGenerator(cfg, 1000)
	b := NewGenerator(cfg, 1000)
	for i := 0; i < 1000; i++ {
		ka, ada := a.Ad(i)
		kb, adb := b.Ad(i)
		if ka != kb || ada.String() != adb.String() {
			t.Fatalf("ad %d differs between two generators with the same seed", i)
		}
	}
	c := NewGenerator(GenConfig{Shape: ShapeJobs, Seed: 2, Owners: cfg.Owners, Projects: cfg.Projects,
		Commands: cfg.Commands, SparseFraction: cfg.SparseFraction, Window: cfg.Window, Now: cfg.Now}, 1000)
	same := 0
	for i := 0; i < 1000; i++ {
		_, adc := c.Ad(i)
		_, ada := a.Ad(i)
		if adc.String() == ada.String() {
			same++
		}
	}
	if same == 1000 {
		t.Fatal("a different seed produced an identical dataset; the seed does nothing")
	}
}
