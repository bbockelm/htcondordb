package scheddsync

import (
	"fmt"
	"os"
	"sort"
	"testing"

	"github.com/PelicanPlatform/classad/db"
)

// Where a sample's bytes actually go, and -- the question that has to be settled first -- whether
// the bytes being measured are COLUMNARIZED at all. Two ways the storage number can be quietly
// wrong, both of which look like "the table is just big":
//
//   - The active (unsealed) segment is never columnarized, so an average over the whole table is
//     diluted by it. At CI size that is one segment in eight and worth ~25%.
//   - A field can be in the schema and still escape to row form on most records (wrong width,
//     wrong kind, absent). SchemaFit reports that per field; a high escape rate means the column
//     exists and is doing nothing.

// fieldGroup buckets a schema field by what it is for, so the composition report says something
// actionable ("the derived rates are half the record") rather than listing 41 names.
func fieldGroup(name string) string {
	switch c, ok := sampleAttrs[canonAttr(name)]; {
	case ok && c == classContext:
		return "context"
	case ok && c == classHWM:
		return "high-water mark"
	case ok && (c == classRunCounter || c == classCrossRunCounter):
		return "raw counter"
	case ok && c == classGauge:
		return "gauge"
	}
	switch name {
	case SampleTimeAttr, SampleIntervalAttr, SampleTriggerAttr, SampleBaselineAttr,
		SampleLogSeqAttr, RunInstanceAttr, "SampleTimeFromIngest":
		return "sample metadata"
	}
	for _, r := range derivedRates {
		if r.Out == name {
			return "derived rate"
		}
	}
	for _, r := range derivedRatios {
		if r.Out == name {
			return "derived rate"
		}
	}
	if name == "GpuUtil" {
		return "derived rate"
	}
	return "other"
}

// TestJobMetricsRecordComposition answers "what is a sample made of" and, first, proves the
// accelerator actually engaged -- otherwise the byte attribution below describes row storage.
func TestJobMetricsRecordComposition(t *testing.T) {
	// A DIFFERENT population shape from the size measurement, on purpose. That one needs
	// production's locality (many jobs, few samples each) or its bytes/record is meaningless.
	// This one needs enough samples PER JOB that the rate-less first samples are a small share --
	// otherwise the derived rates never clear the schema's presence threshold and the assertion
	// below is inert. Both regimes are real; each test uses the one its own claim depends on.
	jobs, perJob := 4000, 24
	if os.Getenv("HTCONDORDB_SCALE") != "" {
		jobs, perJob = 20000, 24
	}
	cat, err := db.OpenCatalog(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer cat.Close()
	arch, err := cat.CreateArchiveTable("job_metrics", db.ArchiveConfig{
		CategoricalAttrs: JobMetricsCategoricalAttrs,
		ValueAttrs:       JobMetricsValueAttrs,
		ZoneAttrs:        JobMetricsZoneAttrs,
	})
	if err != nil {
		t.Fatal(err)
	}
	nBaselines := buildPopulationBaselines(t, arch, jobs, perJob, 900)
	rawPerRec := float64(arch.Stats().UsedBytes) / float64(arch.Count())
	arch.Reindex()
	arch.BuildAndEnableSchemaScan(2000, 32)

	info := arch.SchemaScanInfo()
	if !info.Enabled || len(info.Schema) == 0 {
		t.Fatal("accelerator not enabled: everything below would describe ROW storage")
	}
	// Every sealed segment must carry a columnar block. A partial coverage number means the
	// average is mixing two storage formats.
	if info.CoveredSegments != info.SealedSegments {
		t.Errorf("columnar coverage %d/%d sealed segments -- the byte attribution mixes formats",
			info.CoveredSegments, info.SealedSegments)
	}

	st := arch.Stats()
	n := arch.Count()
	perRec := float64(st.UsedBytes) / float64(n)
	// The active segment is still row-form. Subtract its (estimated) share so the headline is the
	// cost of COLUMNARIZED data, which is what a table in steady state is made of. Segments are
	// equal-sized in row form, so the active one holds about n/segments records at the raw rate.
	activeRecs := float64(n) / float64(st.Segments)
	sealedPerRec := (float64(st.UsedBytes) - activeRecs*rawPerRec) / (float64(n) - activeRecs)
	t.Logf("records=%d segments=%d (%d sealed, %d columnar) raw=%.0fB/rec whole-table=%.0fB/rec "+
		"COLUMNARIZED=%.0fB/rec", n, st.Segments, info.SealedSegments, info.CoveredSegments,
		rawPerRec, perRec, sealedPerRec)

	// Fixed slot widths, by purpose. This is the UNCOMPRESSED columnar cost per record -- the
	// upper bound the codec then works against, and the thing a projection change moves.
	type grp struct {
		fields, width int
	}
	groups := map[string]*grp{}
	total := 0
	for _, f := range info.Schema {
		g := fieldGroup(f.Name)
		if groups[g] == nil {
			groups[g] = &grp{}
		}
		w := f.Width
		if f.Kind == "bool" {
			w = 0 // bit-packed
		}
		groups[g].fields++
		groups[g].width += w
		total += w
	}
	names := make([]string, 0, len(groups))
	for k := range groups {
		names = append(names, k)
	}
	sort.Slice(names, func(i, j int) bool { return groups[names[i]].width > groups[names[j]].width })
	t.Logf("schema: %d fields, %dB of fixed slots per record (uncompressed)", len(info.Schema), total)
	for _, k := range names {
		g := groups[k]
		t.Logf("   %-16s %2d fields %4dB/rec  %4.1f%% of the slots", k, g.fields, g.width,
			100*float64(g.width)/float64(total))
	}
	// Which attributes the projection writes that the schema did NOT take. These are stored in
	// row form on every record, so they are where the gap between the slot total above and the
	// measured bytes/record goes.
	inSchema := map[string]bool{}
	byKind := map[string]int{}
	for _, f := range info.Schema {
		inSchema[canonAttr(f.Name)] = true
		byKind[f.Kind]++
	}
	t.Logf("schema kinds: %v", byKind)

	// THE PRESENCE CLIFF. A field enters the schema only if it is present on >=90% of sampled
	// records. A derived rate is absent on a run's rate-less samples (its first ones -- no usable
	// predecessor), so whether the rate columns columnarize at all depends on how many times the
	// POOL's jobs get sampled: a job sampled 24 times carries rates on 96% of its records and the
	// columns are taken; a pool of short jobs sampled 4 times sits at 75% and every rate column
	// falls out -- losing both the storage and, worse, the columnar fast path for exactly the
	// columns a dashboard aggregates.
	//
	// The sampler already exports the signal for this: baseline/appended IS the absence fraction.
	// So assert the RULE rather than a size -- when the observed baseline share is small enough,
	// the rates must be columnar.
	ms := arch.Count()
	baselineFrac := float64(nBaselines) / float64(ms)
	ratesIn := 0
	for _, r := range derivedRates {
		if inSchema[r.Out] {
			ratesIn++
		}
	}
	t.Logf("baseline share %.1f%% -> %d/%d derived-rate columns columnarized",
		100*baselineFrac, ratesIn, len(derivedRates))
	if baselineFrac < 0.10 && ratesIn == 0 {
		t.Errorf("only %.1f%% of samples are rate-less, yet NO derived rate columnarized: the "+
			"columns a dashboard aggregates are being stored as rows", 100*baselineFrac)
	}
	var missing []string
	for _, f := range projectedAttrNames() {
		if !inSchema[canonAttr(f)] {
			missing = append(missing, f)
		}
	}
	sort.Strings(missing)
	t.Logf("projected but NOT columnarized (%d): %v", len(missing), missing)
	t.Log("NOTE: most of those are attributes this population never sets (NetworkIn, GPU, " +
		"Recent*) and are correctly absent; the ones that matter are any the sampler DID write.")

	// A field that escapes is in the schema and not in the columns: it pays the slot AND the row
	// bytes. A few percent is normal (an attribute a universe does not publish); a field escaping
	// on most records means the projection is writing something the columnar format cannot hold,
	// which is exactly how this table would silently go back to row size.
	fits, sampled := arch.SchemaFit(2000)
	var bad []string
	for _, f := range fits {
		if f.Escaped-f.Missing > 0.10 {
			bad = append(bad, fmt.Sprintf("%s(kind=%s w=%d escaped=%.0f%% missing=%.0f%%)",
				f.Name, f.Kind, f.Width, 100*f.Escaped, 100*f.Missing))
		}
	}
	t.Logf("schema fit over %d sampled records: %d fields, %d escaping for a reason a re-schema "+
		"could fix", sampled, len(fits), len(bad))
	if len(bad) > 0 {
		t.Errorf("fields stored as rows despite being in the schema: %v", bad)
	}
}

// projectedAttrNames is every attribute a sample can carry, for checking which of them the
// derived schema actually took.
func projectedAttrNames() []string {
	var out []string
	for n := range sampleAttrs {
		out = append(out, n)
	}
	for _, r := range derivedRates {
		out = append(out, r.Out)
	}
	for _, r := range derivedRatios {
		out = append(out, r.Out)
	}
	out = append(out, SampleTimeAttr, SampleIntervalAttr, SampleTriggerAttr, SampleBaselineAttr,
		SampleLogSeqAttr, RunInstanceAttr, "GpuUtil", "ProjectName")
	return out
}

// TestJobMetricsGroupContribution attributes real stored bytes to each part of the projection by
// removing it and re-measuring. Slot widths (above) say what a group costs uncompressed; this says
// what it costs after the codec, which is the number that decides whether dropping it is worth it.
func TestJobMetricsGroupContribution(t *testing.T) {
	if testing.Short() {
		t.Skip("builds the population once per arm")
	}
	jobs, perJob := scaleSize(t)
	measure := func(t *testing.T, label string, drop map[string]bool, noRates bool) float64 {
		t.Helper()
		saved := map[string]attrClass{}
		for name := range drop {
			if c, ok := sampleAttrs[name]; ok {
				saved[name] = c
				delete(sampleAttrs, name)
			}
		}
		savedRates := derivedRates
		savedRatios := derivedRatios
		if noRates {
			derivedRates, derivedRatios = nil, nil
		}
		defer func() {
			for k, v := range saved {
				sampleAttrs[k] = v
			}
			derivedRates, derivedRatios = savedRates, savedRatios
		}()

		cat, err := db.OpenCatalog(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer cat.Close()
		arch, err := cat.CreateArchiveTable("job_metrics", db.ArchiveConfig{
			CategoricalAttrs: JobMetricsCategoricalAttrs,
			ValueAttrs:       JobMetricsValueAttrs,
			ZoneAttrs:        JobMetricsZoneAttrs,
		})
		if err != nil {
			t.Fatal(err)
		}
		buildPopulation(t, arch, jobs, perJob, 900)
		raw := float64(arch.Stats().UsedBytes) / float64(arch.Count())
		arch.Reindex()
		arch.BuildAndEnableSchemaScan(2000, 32)
		st := arch.Stats()
		n := float64(arch.Count())
		activeRecs := n / float64(st.Segments)
		sealed := (float64(st.UsedBytes) - activeRecs*raw) / (n - activeRecs)
		t.Logf("%-28s %6.0fB/rec columnarized", label, sealed)
		return sealed
	}

	strs := map[string]bool{"GlobalJobId": true, "User": true, "RemoteHost": true}
	counters := map[string]bool{}
	for name, c := range sampleAttrs {
		if c == classRunCounter || c == classCrossRunCounter {
			counters[name] = true
		}
	}
	full := measure(t, "everything (shipped)", nil, false)
	noStr := measure(t, "minus identity strings", strs, false)
	noRates := measure(t, "minus derived rates", nil, true)
	noCounters := measure(t, "minus raw counters", counters, false)
	t.Logf("contribution: identity strings %.0fB, derived rates %.0fB, raw counters %.0fB "+
		"(of %.0fB/rec)", full-noStr, full-noRates, full-noCounters, full)
}
