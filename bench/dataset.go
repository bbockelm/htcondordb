package bench

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/PelicanPlatform/classad/classad"
)

// Generating a dataset to benchmark against.
//
// THE PROPERTY THAT MATTERS IS SCALE INVARIANCE. A sweep exists to vary ONE thing -- the size of
// the data, or the hardware under it -- and a generator whose ads change character with the row
// count varies a second thing silently. This is not hypothetical: a fixture in this tree defines
// `ResidentSetSize = 90000 + i`, which makes `MemoryUsage = (ResidentSetSize + 1023) / 1024` a
// value whose RANGE is a function of how many rows were generated. Benchmark that at 20k and at
// 2M and the column's width, its compressibility and its zone maps all differ -- and the
// difference reads as a property of scale.
//
// So the generator splits its attributes in two, and the split is the whole design:
//
//   - INHERENTLY scale-dependent, because the data really is: key identity (ClusterId, ProcId,
//     GlobalJobId) must stay distinct, and timestamps must advance. These are generated from the
//     row index and documented as such.
//   - Everything else is drawn from a distribution fixed by GenConfig and never from the row
//     index: value ranges, string cardinalities, how often a rare attribute appears. Doubling the
//     row count must not move any of them. TestDatasetIsScaleInvariant asserts exactly this.
//
// TIMESTAMPS SPAN A WINDOW, not one second per row. A real history covers a period; generating
// one row per second makes the window a function of N, which is the same mistake in another
// place, and it makes every zone map trivially selective in a way no real archive is.
//
// REALISM IS ABOUT SHAPE, NOT VOLUME. What makes a read expensive is the attribute count, the
// mix of literals and expressions, string cardinality and sparsity -- not whether the strings say
// anything. MemoryUsage is an expression here because it is an expression in 1996 of 2000 ads on
// a real access point, and an ad full of only literals would measure a store nobody runs.

// Shape is which of a deployment's tables the ads imitate. The names match the tables a live
// server actually serves, so a client sweep can point at either this data or the real thing.
type Shape string

const (
	// ShapeJobs is the mutable job-queue table: ads for jobs still in the queue.
	ShapeJobs Shape = "jobs"
	// ShapeHistory is the completed-job archive, append-only and read newest-first.
	ShapeHistory Shape = "history"
	// ShapeEpochHistory is the per-run archive: one ad per execution attempt, so it is wider in
	// run-time attributes and narrower in submission ones.
	ShapeEpochHistory Shape = "epoch_history"
)

// GenConfig fixes the distributions. Every field here is independent of the row count, which is
// what makes two sweeps at different sizes comparable.
type GenConfig struct {
	Shape Shape
	// Seed makes a dataset reproducible: the same seed and config give byte-identical ads, so a
	// comparison between two machines is a comparison of the machines.
	Seed int64
	// Owners, Projects and Commands are string CARDINALITIES -- how many distinct values exist,
	// not how many rows. Cardinality drives dictionary encoding and index size, and holding it
	// fixed while rows grow is the realistic case: a pool gains jobs faster than users.
	Owners   int
	Projects int
	Commands int
	// SparseFraction is how often a rarely-present attribute appears. A sparse attribute is read
	// from the RECORD rather than a column, so this controls how much of a projected read escapes
	// the columnar path -- the single largest lever on read cost, and one a generator that
	// omitted it would hide entirely.
	SparseFraction float64
	// Window is the period the generated timestamps span, independent of the row count.
	Window time.Duration
	// Now is the newest timestamp; the window extends backward from it. Fixed rather than
	// time.Now() so a dataset is reproducible.
	Now time.Time
}

// DefaultGenConfig is a realistic access point: a few hundred users, a long tail of commands, a
// week of history.
func DefaultGenConfig(shape Shape) GenConfig {
	return GenConfig{
		Shape:          shape,
		Seed:           1,
		Owners:         256,
		Projects:       32,
		Commands:       64,
		SparseFraction: 0.05,
		Window:         7 * 24 * time.Hour,
		Now:            time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

// Generator produces ads for a shape. It is not safe for concurrent use; a caller filling a store
// from several goroutines gives each its own, seeded differently.
type Generator struct {
	cfg  GenConfig
	rng  *rand.Rand
	rows int
}

// NewGenerator returns a generator for rows ads. rows is used ONLY to spread timestamps across
// the configured window -- no attribute's value distribution depends on it.
func NewGenerator(cfg GenConfig, rows int) *Generator {
	if cfg.Owners <= 0 {
		cfg.Owners = 1
	}
	if cfg.Projects <= 0 {
		cfg.Projects = 1
	}
	if cfg.Commands <= 0 {
		cfg.Commands = 1
	}
	if rows <= 0 {
		rows = 1
	}
	return &Generator{cfg: cfg, rng: rand.New(rand.NewSource(cfg.Seed)), rows: rows}
}

// Ad returns the i'th ad. i supplies key identity and the timestamp's position in the window;
// every other value comes from the generator's own stream.
func (g *Generator) Ad(i int) (key string, ad *classad.ClassAd) {
	var b strings.Builder
	r := g.rng

	// Scale-dependent BY NECESSITY: the key must be distinct, and the clock must advance.
	cluster := 1000 + i/10
	proc := i % 10
	// Spread across the window rather than one row per second, so the window is a property of the
	// config and not of the row count.
	frac := float64(i) / float64(g.rows)
	ts := g.cfg.Now.Add(-time.Duration((1 - frac) * float64(g.cfg.Window))).Unix()

	// Everything below is drawn from fixed distributions.
	owner := r.Intn(g.cfg.Owners)
	project := r.Intn(g.cfg.Projects)
	cmd := r.Intn(g.cfg.Commands)
	cpus := 1 + r.Intn(8)
	memMB := 1024 + r.Intn(16)*512
	diskKB := 10240 + r.Intn(64)*1024
	status := 1 + r.Intn(5)
	rss := 50000 + r.Intn(100000)
	imageSize := rss + r.Intn(20000)
	wall := r.Intn(72 * 3600)
	starts := 1 + r.Intn(3)
	exitCode := 0
	if r.Float64() < 0.08 {
		exitCode = 1 + r.Intn(40)
	}

	fmt.Fprintf(&b, "ClusterId = %d\nProcId = %d\n", cluster, proc)
	fmt.Fprintf(&b, "Owner = \"user%03d\"\nAccountingGroup = \"group%02d.user%03d\"\n", owner, project%8, owner)
	fmt.Fprintf(&b, "ProjectName = \"proj%02d\"\n", project)
	fmt.Fprintf(&b, "JobStatus = %d\nJobUniverse = 5\nJobPrio = %d\n", status, r.Intn(20)-10)
	fmt.Fprintf(&b, "RequestCpus = %d\nRequestMemory = %d\nRequestDisk = %d\n", cpus, memMB, diskKB)
	fmt.Fprintf(&b, "ImageSize = %d\nResidentSetSize = %d\nDiskUsage = %d\n", imageSize, rss, diskKB/2)
	fmt.Fprintf(&b, "Cmd = \"/opt/payload/run-%02d.sh\"\nIwd = \"/home/user%03d\"\n", cmd, owner)
	fmt.Fprintf(&b, "GlobalJobId = \"bench.example.edu#%d.%d#%d\"\n", cluster, proc, ts)
	fmt.Fprintf(&b, "QDate = %d\nEnteredCurrentStatus = %d\n", ts, ts)
	fmt.Fprintf(&b, "NumJobStarts = %d\nNumShadowStarts = %d\n", starts, starts)
	fmt.Fprintf(&b, "BytesSent = %d\nBytesRecvd = %d\n", r.Intn(1<<28), r.Intn(1<<26))
	// An EXPRESSION over a sibling, not a literal: the common real case, and the one that makes a
	// projected read evaluate rather than copy.
	b.WriteString("MemoryUsage = (ResidentSetSize + 1023) / 1024\n")
	b.WriteString("Requirements = TARGET.Arch == \"X86_64\" && TARGET.Memory >= RequestMemory\n")

	switch g.cfg.Shape {
	case ShapeHistory, ShapeEpochHistory:
		fmt.Fprintf(&b, "CompletionDate = %d\nJobStartDate = %d\n", ts, ts-int64(wall))
		fmt.Fprintf(&b, "RemoteWallClockTime = %d.0\nExitCode = %d\nExitBySignal = false\n", wall, exitCode)
		fmt.Fprintf(&b, "LastRemoteHost = \"slot1_%d@node%03d.example.edu\"\n", 1+r.Intn(8), r.Intn(512))
		fmt.Fprintf(&b, "CpusUsage = %d.%02d\n", r.Intn(cpus), r.Intn(100))
		if g.cfg.Shape == ShapeEpochHistory {
			fmt.Fprintf(&b, "NumRestarts = %d\nEpochId = %d\n", r.Intn(3), i%7)
		}
	case ShapeJobs:
		fmt.Fprintf(&b, "JobCurrentStartDate = %d\nServerTime = %d\n", ts, ts)
	}

	// Sparse attributes: present on a fixed fraction of ads, so a projection naming one escapes
	// the columnar path at a rate the config sets rather than one the row count sets.
	if r.Float64() < g.cfg.SparseFraction {
		fmt.Fprintf(&b, "HoldReasonCode = %d\nHoldReason = \"benchmark hold %d\"\n", 1+r.Intn(20), r.Intn(5))
	}
	if r.Float64() < g.cfg.SparseFraction {
		fmt.Fprintf(&b, "TransferInputSizeMB = %d\n", r.Intn(4096))
	}

	ad, err := classad.ParseOld(b.String())
	if err != nil {
		// The generator writes its own text; a parse failure is a bug here, not bad input.
		panic(fmt.Sprintf("bench: generated ad does not parse: %v", err))
	}
	return fmt.Sprintf("%d.%d", cluster, proc), ad
}
