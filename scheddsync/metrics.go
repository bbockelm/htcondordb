package scheddsync

import (
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
)

// Job resource-usage sampling.
//
// The schedd's shadow pushes a job's resource counters into the job queue on every update it
// makes -- periodic (SHADOW_QUEUE_UPDATE_INTERVAL, default 900s), on every job-state change, on
// each checkpoint, and at termination -- because HTCondor's whitelist of queue-updatable
// attributes (QmgrJobUpdater::common_job_queue_attrs) is included in EVERY update type, not just
// the periodic one. JobSync already parses all of those commits, so a resource-usage time series
// costs no new polling: one sample per committed transaction that moved a usage attribute,
// appended to an archive table that rotates oldest-first.
//
// Two properties of HTCondor's counters shape everything here, and both are counter-intuitive:
//
//   - The memory numbers are HIGH-WATER MARKS, not gauges. MemoryUsage / ResidentSetSize /
//     ImageSize only ever ratchet up (the starter maxes them, then the shadow maxes them again),
//     so a "memory over time" plot is a staircase. Worse, the shadow SEEDS them from the previous
//     run's value, so for RunInstanceID > 0 they are a whole-job maximum wearing this run's
//     timestamp -- that run's real peak was never written anywhere. Consumers must filter to
//     RunInstanceID == 0 to ask a per-run memory question.
//   - The counters do NOT all reset at run start. RemoteUserCpu/RemoteSysCpu are explicitly
//     zeroed; DiskUsage effectively resets on the first starter report; BytesSent/BytesRecvd are
//     deliberately carried across runs; the memory marks are carried too. So a delta is only
//     meaningful within one RunInstanceID, except for the few attributes classed
//     classCrossRunCounter below.
//
// Rates are therefore derived HERE, at ingest, and not at query time: the SQL surface has no
// window functions, no LAG and no self-join, so rate(counter) is simply not expressible as a
// query, and the sampler is the one place that holds both the current and the previous
// observation.

const (
	// SampleTimeAttr is the observation instant: the starter's own clock for when it gathered
	// the numbers (StatsLastUpdateTimeStarter), NOT our ingest clock. Using the starter's
	// timestamp absorbs the shadow's flush delay and this tailer's lag, both of which would
	// otherwise smear the series. It falls back to the ingest clock only when the job carries no
	// starter stats (a universe that publishes none, or a very old shadow).
	SampleTimeAttr = "SampleTime"
	// SampleIntervalAttr is the seconds between this observation and the previous one for the
	// same run, with suspended time subtracted -- the denominator every derived rate uses. Absent
	// on a run's first observation.
	SampleIntervalAttr = "SampleInterval"
	// SampleTriggerAttr names what caused the commit this sample was taken from, so a panel can
	// narrow to an even series ("periodic") or find run endpoints ("terminal") without a join.
	SampleTriggerAttr = "SampleTrigger"
	// SampleBaselineAttr marks an observation that has no usable predecessor and therefore
	// carries no derived rates: a run's first sample, or the first one after a daemon restart.
	// It is set rather than left implicit so a gap in a rate series is explainable from the data.
	SampleBaselineAttr = "SampleBaseline"
	// SampleLogSeqAttr records the job_queue.log generation (the schedd's op-107
	// LogHistoricalSequenceNumber, bumped on every compaction) the sample came from. Provenance
	// only -- it is not the dedup key; see dedupConstraint.
	SampleLogSeqAttr = "LogSeq"
)

// DefaultJobMetricsTable is the archive table job samples are appended to.
const DefaultJobMetricsTable = "job_metrics"

// numShadowStartsAttr is the schedd-maintained count of shadows spawned for a job. HTCondor's own
// epoch writer derives the run-instance id from it as NumShadowStarts-1
// (job_ad_instance_recording.cpp), so deriving RunInstanceID the same way is what lets a sample
// series and an epoch_history record refer to the same run without translation.
const numShadowStartsAttr = "NumShadowStarts"

// statsClockAttr is the starter's own "when I gathered these" timestamp, renamed by the shadow so
// it does not collide with schedd statistics of the same name.
const statsClockAttr = "StatsLastUpdateTimeStarter"

// attrClass says how an attribute behaves over time, which decides whether differentiating it is
// meaningful and across what span.
type attrClass uint8

const (
	// classContext is identity, axes and denominators: copied verbatim, never differentiated.
	classContext attrClass = iota
	// classHWM is a high-water mark: monotone, and for the memory family carried across runs.
	// Copied verbatim; a ratio against its Request* denominator is derived, but never a rate.
	classHWM
	// classRunCounter is cumulative WITHIN a run and reset at run start. Differentiating it
	// across a RunInstanceID boundary is meaningless, so the sampler refuses to.
	classRunCounter
	// classCrossRunCounter is cumulative across runs by design (the shadow seeds it from the job
	// ad). A delta spanning a run boundary is correct for these, and only these.
	classCrossRunCounter
	// classGauge is instantaneous, or already a rate over the starter's own recent window.
	// Copied verbatim and plotted directly.
	classGauge
)

// sampleAttrs is the projection: every attribute copied from the job row into a sample, and how it
// behaves. Keeping it a fixed, flat, all-scalar set is deliberate -- it is what lets the archive's
// columnar segments carry essentially the whole record in columns (nested ClassAds such as
// TransferInputStats fall back to row form, which is why they are excluded).
//
// Units, because they are not uniform and a ratio built from the wrong pair is silently wrong:
// MemoryUsage is MiB; ResidentSetSize / ProportionalSetSize / ImageSize / DiskUsage are KiB;
// RequestMemory is MiB and RequestDisk is KiB (so MemoryUsage/RequestMemory and
// DiskUsage/RequestDisk are the two ratios whose units agree); CPU is seconds; block I/O is bytes
// (the *Kbytes variants are KiB); NetworkIn/NetworkOut are MB and are only populated for
// container universes.
var sampleAttrs = map[string]attrClass{
	// identity, axes, denominators
	"ClusterId":                    classContext,
	"ProcId":                       classContext,
	"GlobalJobId":                  classContext,
	"Owner":                        classContext,
	"User":                         classContext,
	"RemoteHost":                   classContext,
	"JobStatus":                    classContext,
	"JobUniverse":                  classContext,
	"JobCurrentStartExecutingDate": classContext,
	"NumJobStarts":                 classContext,
	numShadowStartsAttr:            classContext,
	"RequestCpus":                  classContext,
	"RequestMemory":                classContext,
	"RequestDisk":                  classContext,
	"RequestGpus":                  classContext,
	"TotalSuspensions":             classContext,
	"StatsLifetimeStarter":         classContext,

	// high-water marks
	"MemoryUsage":         classHWM,
	"ResidentSetSize":     classHWM,
	"ProportionalSetSize": classHWM,
	"ImageSize":           classHWM,
	"DiskUsage":           classHWM,
	"ScratchDirFileCount": classHWM,

	// cumulative within a run
	"RemoteUserCpu":      classRunCounter,
	"RemoteSysCpu":       classRunCounter,
	"BlockReadBytes":     classRunCounter,
	"BlockWriteBytes":    classRunCounter,
	"BlockReads":         classRunCounter,
	"BlockWrites":        classRunCounter,
	"NetworkIn":          classRunCounter,
	"NetworkOut":         classRunCounter,
	"JobCpuInstructions": classRunCounter,

	// cumulative across runs
	"CumulativeRemoteUserCpu":  classCrossRunCounter,
	"CumulativeRemoteSysCpu":   classCrossRunCounter,
	"BytesSent":                classCrossRunCounter,
	"BytesRecvd":               classCrossRunCounter,
	"CumulativeSuspensionTime": classCrossRunCounter,
	"CommittedTime":            classCrossRunCounter,
	"CommittedSlotTime":        classCrossRunCounter,
	"CumulativeTransferTime":   classCrossRunCounter,

	// instantaneous, or already windowed by the starter
	"IOWait":                     classGauge,
	"JobVMCpuUtilization":        classGauge,
	"CpusUsage":                  classGauge,
	"GPUsAverageUsage":           classGauge,
	"RecentBlockReadBytes":       classGauge,
	"RecentBlockWriteBytes":      classGauge,
	"RecentBlockReads":           classGauge,
	"RecentBlockWrites":          classGauge,
	"RecentStatsLifetimeStarter": classGauge,
}

// derivedRate is one rate column computed at ingest: the summed delta of In over the sample
// interval. CrossRun marks the few whose inputs accumulate across runs (classCrossRunCounter), so
// a delta spanning a run boundary is still correct; every other rate is suppressed at the seam.
type derivedRate struct {
	Out      string
	In       []string
	CrossRun bool
	// Doc is what the column means, kept next to the definition so the docs and the code cannot
	// drift apart. Not emitted.
	Doc string
}

var derivedRates = []derivedRate{
	{Out: "CpuUtil", In: []string{"RemoteUserCpu", "RemoteSysCpu"},
		Doc: "cores used over the interval (CPU-seconds per second)"},
	{Out: "BlockReadRate", In: []string{"BlockReadBytes"}, Doc: "bytes/s read from disk"},
	{Out: "BlockWriteRate", In: []string{"BlockWriteBytes"}, Doc: "bytes/s written to disk"},
	{Out: "BlockReadOpRate", In: []string{"BlockReads"}, Doc: "read ops/s"},
	{Out: "BlockWriteOpRate", In: []string{"BlockWrites"}, Doc: "write ops/s"},
	{Out: "NetworkInRate", In: []string{"NetworkIn"}, Doc: "MB/s in (container universes only)"},
	{Out: "NetworkOutRate", In: []string{"NetworkOut"}, Doc: "MB/s out (container universes only)"},
	{Out: "InstructionRate", In: []string{"JobCpuInstructions"}, Doc: "CPU instructions/s (Linux)"},
	// Split rather than summed: a multi-input rate needs EVERY input reported (see sumDelta), so
	// summing these would make the transfer rate vanish whenever only one direction was active.
	// They also accumulate across runs, which is what makes them safe to differentiate at a seam.
	{Out: "BytesSentRate", In: []string{"BytesSent"}, CrossRun: true,
		Doc: "bytes/s sent to the execute node (input transfer)"},
	{Out: "BytesRecvdRate", In: []string{"BytesRecvd"}, CrossRun: true,
		Doc: "bytes/s received from the execute node (output transfer)"},
}

// derivedRatio is a fraction of a resource's usage to its request. No predecessor is needed, so
// these are emitted on every sample including a baseline one. Num and Den must share units.
type derivedRatio struct {
	Out, Num, Den string
}

var derivedRatios = []derivedRatio{
	// Both MiB. This is a HIGH-WATER-MARK ratio (see the package comment): for RunInstanceID > 0
	// the numerator is the maximum over all of the job's runs, not this one's.
	{Out: "MemUtil", Num: "MemoryUsage", Den: "RequestMemory"},
	// Both KiB.
	{Out: "DiskUtil", Num: "DiskUsage", Den: "RequestDisk"},
}

// sampleTrigger classifies the commit a sample was taken from. Ordered by precedence: when one
// transaction carries several kinds of change, the highest wins, because that is the one a reader
// would name it by.
type sampleTrigger uint8

const (
	triggerNone sampleTrigger = iota
	// triggerChirp: only an admin-configured extra attribute moved -- i.e. the job wrote its own
	// metric via condor_chirp and nothing else changed.
	triggerChirp
	// triggerUpdate: a usage attribute moved with no other signal (the shadow flushed without the
	// starter's stats clock advancing).
	triggerUpdate
	// triggerPeriodic: the starter's stats clock advanced, so these are genuinely fresh numbers.
	triggerPeriodic
	// triggerStatus: the job changed state (started executing, suspended, began/finished a
	// transfer, reconnected).
	triggerStatus
	// triggerCheckpoint: a checkpoint completed.
	triggerCheckpoint
	// triggerTerminal: the run ended (completed, evicted, held, removed). The sample taken from
	// this commit is the run's endpoint -- the reason a resource plot needs no epoch_history
	// lookup to find where a run finished.
	triggerTerminal
)

func (t sampleTrigger) String() string {
	switch t {
	case triggerChirp:
		return "chirp"
	case triggerUpdate:
		return "update"
	case triggerPeriodic:
		return "periodic"
	case triggerStatus:
		return "status"
	case triggerCheckpoint:
		return "checkpoint"
	case triggerTerminal:
		return "terminal"
	}
	return "none"
}

// triggerFor classifies one attribute name. Attributes not named here and not in sampleAttrs
// contribute nothing (triggerNone) unless the admin configured them, which the caller handles.
func triggerFor(name string) sampleTrigger {
	switch {
	case strings.EqualFold(name, statsClockAttr):
		return triggerPeriodic
	}
	// Case-insensitive, because ClassAd attribute names are.
	switch strings.ToLower(name) {
	case "exitcode", "exitbysignal", "exitsignal", "exitreason", "jobexitstatus",
		"terminationpending", "removereason", "holdreason", "holdreasoncode",
		"vacatereason", "lastvacatetime", "completiondate":
		return triggerTerminal
	case "jobcheckpointnumber", "lastcheckpointtime", "numckpts":
		return triggerCheckpoint
	case "jobstatus", "transferringinput", "transferringoutput", "transferqueued",
		"lastsuspensiontime", "numjobstarts", "numshadowstarts",
		"jobcurrentstartexecutingdate", "jobcurrentreconnectattempt":
		return triggerStatus
	}
	// Only a USAGE attribute makes an otherwise-unremarkable commit worth sampling. The context
	// attributes (Owner, RequestMemory, ClusterId, ...) are carried on every sample but are not
	// themselves observations, and treating them as triggers would sample every job at submit
	// time -- a record per job in a 100k-job submit burst, none of which has run yet.
	if c, ok := sampleAttrs[canonAttr(name)]; ok && c != classContext {
		return triggerUpdate
	}
	return triggerNone
}

// canonAttr resolves an attribute name to the spelling used as a key in sampleAttrs. ClassAd
// names are case-insensitive but Go maps are not, so a lookup has to go through here; the map is
// small and the result is cached per process.
func canonAttr(name string) string {
	if _, ok := sampleAttrs[name]; ok {
		return name
	}
	if c, ok := attrFold[strings.ToLower(name)]; ok {
		return c
	}
	return name
}

var attrFold = func() map[string]string {
	m := make(map[string]string, len(sampleAttrs))
	for k := range sampleAttrs {
		m[strings.ToLower(k)] = k
	}
	return m
}()

// usageSuffixes are the endings HTCondor's shadow itself uses to decide that an attribute from the
// starter is a resource metric worth forwarding to the queue (remoteresource.cpp). Matching them
// here is what picks up custom machine resources -- GPUsUsage, GPUsAverageUsage and any
// STARTD_CRON metric -- without naming each one, since which of them exist depends on how the
// pool's GPU monitor is configured.
var usageSuffixes = []string{"Usage", "AverageUsage"}

func hasUsageSuffix(name string) bool {
	for _, s := range usageSuffixes {
		if len(name) > len(s) && strings.HasSuffix(name, s) {
			return true
		}
	}
	return false
}

// prevObservation is the last sample emitted for one job: everything needed to differentiate the
// next one. It is a CACHE, not a record -- it is rebuilt from the stream after a restart (the
// first post-restart sample per job is simply a baseline with no rates), and nothing durable
// depends on it. Bounded by the number of RUNNING jobs, not by queue size: entries are dropped
// when a run ends or the job leaves the queue.
type prevObservation struct {
	run        int64
	sampleTime int64
	suspended  float64            // CumulativeSuspensionTime, to net out suspended time
	elapsed    float64            // SampleTime - JobCurrentStartExecutingDate, for de-averaging GPU
	counters   map[string]float64 // raw values of every classRunCounter/classCrossRunCounter present
	gpuAvg     float64
	hasGPU     bool
	jobStatus  int64
}

// JobMetricsConfig configures the sampler.
type JobMetricsConfig struct {
	// Archive is the table samples are appended to. Nil disables sampling entirely.
	Archive *db.ArchiveTable
	// Attrs names ADDITIONAL job attributes to copy into every sample -- an AccountingGroup, a
	// ProjectName, or a metric the job sets with condor_chirp. Naming one here is the whole cost
	// of turning it into a plottable series, because it is already flowing through
	// job_queue.log; the sampler simply was not looking at it.
	Attrs []string
	// MinInterval throttles redundant samples for one job: a periodic/update/chirp commit closer
	// than this to the job's previous sample is dropped. It is a volume backstop, not a filter --
	// a state change, a run change and a terminal sample are NEVER dropped, because the endpoint
	// and transition guarantees depend on them. 0 disables the throttle.
	MinInterval time.Duration
	Logger      *slog.Logger
}

// jobMetrics samples running jobs' resource counters out of the job_queue.log stream. It is owned
// by a JobSync and is only ever touched from that syncer's goroutine, so it needs no locking of
// its own; the atomic counters exist only because Status() reads them from another one.
type jobMetrics struct {
	archive     *db.ArchiveTable
	log         *slog.Logger
	extra       []string
	extraFold   map[string]struct{}
	minInterval int64 // seconds
	now         func() time.Time

	// pending accumulates, for the transaction currently being replayed, which job keys moved and
	// the strongest trigger seen for each. Collected at commit and cleared on commit or abort.
	pending map[string]sampleTrigger

	// prev is the per-job predecessor cache described on prevObservation.
	prev map[string]*prevObservation

	// dedup is on until the first sample is found NOT to be in the archive already. A restart
	// re-applies the log from the last durable position, which re-produces samples that were
	// already appended; appends are not idempotent, so the replayed prefix has to be skipped.
	// Once one sample is new every later one is too (the stream is replayed in order), so the
	// check -- one archive query each -- turns itself off. Same shape as HistorySync's recovery
	// dedup.
	dedup bool

	mAppended    atomic.Int64
	mThrottled   atomic.Int64
	mBaseline    atomic.Int64
	mResets      atomic.Int64
	mInherited   atomic.Int64
	mDeduped     atomic.Int64
	mAppendFails atomic.Int64
}

// newJobMetrics builds a sampler. A nil cfg.Archive returns nil, and every call site tolerates a
// nil receiver, so sampling is compiled in but costs nothing when unconfigured.
func newJobMetrics(cfg JobMetricsConfig) *jobMetrics {
	if cfg.Archive == nil {
		return nil
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	m := &jobMetrics{
		archive:     cfg.Archive,
		log:         log,
		extra:       splitMetricAttrs(cfg.Attrs),
		minInterval: int64(cfg.MinInterval / time.Second),
		now:         nowFn,
		pending:     map[string]sampleTrigger{},
		prev:        map[string]*prevObservation{},
		dedup:       true,
	}
	m.extraFold = make(map[string]struct{}, len(m.extra))
	for _, a := range m.extra {
		m.extraFold[strings.ToLower(a)] = struct{}{}
	}
	return m
}

// splitMetricAttrs normalizes a configured attribute list: trimmed, non-empty, de-duplicated
// case-insensitively, order preserved.
func splitMetricAttrs(in []string) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, a := range in {
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}
		k := strings.ToLower(a)
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, a)
	}
	return out
}

// note records that key's attribute changed in the transaction being replayed. Called for every
// SetAttribute/DeleteAttribute that routes to the jobs table; cheap, because the common case is a
// map lookup that returns triggerNone and stores nothing.
func (m *jobMetrics) note(key, attr string) {
	if m == nil {
		return
	}
	t := triggerFor(attr)
	if t == triggerNone {
		switch {
		case m.isExtra(attr):
			t = triggerChirp
		case hasUsageSuffix(attr):
			// A custom machine resource (GPUsUsage, a STARTD_CRON metric). Treat it as a real
			// usage update rather than a chirp: it comes from the starter, on the starter's
			// cadence.
			t = triggerUpdate
		default:
			return
		}
	}
	if cur, ok := m.pending[key]; !ok || t > cur {
		m.pending[key] = t
	}
}

func (m *jobMetrics) isExtra(attr string) bool {
	_, ok := m.extraFold[strings.ToLower(attr)]
	return ok
}

// forget drops a job's predecessor cache entry -- called when the job leaves the queue, so the
// cache tracks running jobs rather than growing with everything the tailer has ever seen.
func (m *jobMetrics) forget(key string) {
	if m == nil {
		return
	}
	delete(m.prev, key)
	delete(m.pending, key)
}

// discard drops the pending set without sampling. Used when a transaction is aborted, and when a
// reconcile reload takes over: a reconcile replays the whole log after a compaction and would
// otherwise emit a duplicate sample for every running job, but a compaction is not new
// information about any of them.
func (m *jobMetrics) discard() {
	if m == nil {
		return
	}
	clear(m.pending)
}

// collect builds the samples for the transaction about to be committed, reading each touched job
// back through the open transaction -- so the row is the merged, post-update ad, carrying the
// context attributes (Owner, RequestMemory, NumShadowStarts) that the transaction itself did not
// mention. It is called BEFORE the commit and the result appended only if the commit succeeds, so
// a failed commit leaves no sample for state that never landed.
//
// The predecessor cache is advanced here rather than at append time because the two must agree:
// a sample that is built is the one the next delta is measured from.
func (m *jobMetrics) collect(read rowReader, logSeq int64) []*classad.ClassAd {
	if m == nil || len(m.pending) == 0 {
		return nil
	}
	out := make([]*classad.ClassAd, 0, len(m.pending))
	for key, trig := range m.pending {
		ad, ok := read.LookupClassAd(key)
		if !ok || ad == nil {
			// The row is gone (destroyed in this same transaction). Nothing to sample.
			continue
		}
		if s := m.build(key, trig, ad, logSeq); s != nil {
			out = append(out, s)
		}
	}
	clear(m.pending)
	return out
}

// rowReader is the read side of whatever holds the job rows -- a *db.Txn during a commit, a
// *db.DB in tests. Narrowed to the one method the sampler needs.
type rowReader interface {
	LookupClassAd(key string) (*classad.ClassAd, bool)
}

// build turns one job row into a sample, advancing the predecessor cache. Returns nil when the
// sample is throttled away.
func (m *jobMetrics) build(key string, trig sampleTrigger, job *classad.ClassAd, logSeq int64) *classad.ClassAd {
	run, haveRun := runInstanceOf(job)
	sampleTime, fromStarter := m.sampleTimeOf(job)
	prev := m.prev[key]
	status, _ := job.EvaluateAttrInt("JobStatus")

	// Only jobs that are actually executing have resource usage to observe. A job with no
	// NumShadowStarts has never run; one that is idle or held between runs still has its
	// attributes rewritten by schedd bookkeeping, and sampling those would add points to a
	// series at times the job was consuming nothing. The exception is the terminal commit, which
	// is the run's endpoint and carries its final numbers -- that one is sampled whatever status
	// it leaves the job in (completed, evicted back to idle, or held).
	if !haveRun || (!isExecuting(status) && trig != triggerTerminal) {
		return nil
	}

	// Throttle: only ever drops a sample that carries no transition. A state change, a new run
	// and the run's endpoint always get through, because a plot's annotations and the endpoint
	// guarantee depend on them.
	if m.minInterval > 0 && prev != nil && trig <= triggerPeriodic &&
		prev.run == run && prev.jobStatus == status &&
		sampleTime-prev.sampleTime < m.minInterval {
		m.mThrottled.Add(1)
		return nil
	}

	s := classad.New()
	// Project the fixed attribute set, then the admin's extras. Absent attributes are simply not
	// written -- an undefined column is how a gap renders, and writing a zero would invent data.
	for name, class := range sampleAttrs {
		_ = class
		copyAttr(s, job, name)
	}
	for _, name := range m.extra {
		copyAttr(s, job, name)
	}
	// Custom machine-resource metrics (GPUsUsage, STARTD_CRON metrics): discovered by the same
	// suffix rule HTCondor itself uses, because which ones exist depends on pool configuration.
	for _, name := range job.GetAttributes() {
		if hasUsageSuffix(name) {
			copyAttr(s, job, name)
		}
	}

	_ = s.Set(SampleTimeAttr, sampleTime)
	_ = s.Set(SampleTriggerAttr, trig.String())
	_ = s.Set(SampleLogSeqAttr, logSeq)
	if haveRun {
		_ = s.Set(RunInstanceAttr, run)
	}
	if !fromStarter {
		// Be explicit that this sample's clock is ours, not the starter's: its position on the
		// time axis is only as good as the tailer's lag.
		_ = s.Set("SampleTimeFromIngest", true)
	}

	// Ratios need no predecessor.
	for _, r := range derivedRatios {
		num, ok1 := job.EvaluateAttrNumber(r.Num)
		den, ok2 := job.EvaluateAttrNumber(r.Den)
		if ok1 && ok2 && den > 0 {
			_ = s.Set(r.Out, num/den)
		}
	}

	cur := m.observe(job, run, sampleTime)
	if m.deriveRates(s, prev, cur, run, haveRun) {
		if prev != nil && prev.run != run {
			// A new run whose counters did not restart: the sample carries values inherited from
			// the previous run (the shadow has not yet pushed its reset). Observe-only -- the
			// rates were suppressed anyway by the run-boundary rule -- but a climbing count means
			// the window between the schedd bumping NumShadowStarts and the shadow's first update
			// is wider than expected on this pool.
			if inheritedCounters(prev, cur) {
				m.mInherited.Add(1)
			}
		}
		_ = s.Set(SampleBaselineAttr, true)
		m.mBaseline.Add(1)
	}

	m.prev[key] = cur
	if trig == triggerTerminal {
		// The run ended: this sample is its endpoint, and the next run starts a fresh series.
		delete(m.prev, key)
	}
	return s
}

// observe snapshots the counters this sample will be differentiated from next time.
func (m *jobMetrics) observe(job *classad.ClassAd, run, sampleTime int64) *prevObservation {
	o := &prevObservation{run: run, sampleTime: sampleTime, counters: map[string]float64{}}
	for name, class := range sampleAttrs {
		if class != classRunCounter && class != classCrossRunCounter {
			continue
		}
		if v, ok := job.EvaluateAttrNumber(name); ok {
			o.counters[name] = v
		}
	}
	o.suspended, _ = job.EvaluateAttrNumber("CumulativeSuspensionTime")
	o.jobStatus, _ = job.EvaluateAttrInt("JobStatus")
	if start, ok := job.EvaluateAttrNumber("JobCurrentStartExecutingDate"); ok && start > 0 {
		o.elapsed = float64(sampleTime) - start
	}
	o.gpuAvg, o.hasGPU = job.EvaluateAttrNumber("GPUsAverageUsage")
	return o
}

// deriveRates writes the derived rate columns onto s. It reports whether the sample is a BASELINE
// -- no usable predecessor, so no rates were written -- which is the one case a consumer has to be
// able to tell from a genuine zero.
func (m *jobMetrics) deriveRates(s *classad.ClassAd, prev, cur *prevObservation, run int64, haveRun bool) (baseline bool) {
	if prev == nil || !haveRun {
		return true
	}
	// Elapsed wall time less the time the job spent suspended: a suspended interval advances the
	// clock while the counters stand still, and dividing by the raw elapsed time would report a
	// spuriously low rate rather than no rate.
	interval := float64(cur.sampleTime-prev.sampleTime) - (cur.suspended - prev.suspended)
	if interval <= 0 {
		return true
	}
	_ = s.Set(SampleIntervalAttr, interval)

	sameRun := prev.run == run
	wrote := false
	for _, r := range derivedRates {
		if !sameRun && !r.CrossRun {
			// Every classRunCounter restarts at a run boundary; a delta across it is not a rate.
			continue
		}
		delta, ok := sumDelta(prev, cur, r.In)
		if !ok {
			continue
		}
		if delta < 0 {
			// A counter went backwards: a reset we did not predict, or the starter stopped
			// reporting and started again. Emit no rate rather than a negative one, and count it
			// -- a climbing value means the reset table in this file is missing a case.
			m.mResets.Add(1)
			continue
		}
		_ = s.Set(r.Out, delta/interval)
		wrote = true
	}
	// GPU is a LIFETIME average, not a counter: GPUsAverageUsage is built by the startd as
	// (Uptime - StartOfJobUptime)/(LastUpdate - FirstUpdate). Recovering the per-interval value
	// means un-averaging it over the two elapsed times.
	if sameRun && cur.hasGPU && prev.hasGPU && cur.elapsed > 0 && prev.elapsed > 0 {
		used := cur.gpuAvg*cur.elapsed - prev.gpuAvg*prev.elapsed
		if used >= 0 {
			_ = s.Set("GpuUtil", used/interval)
			wrote = true
		} else {
			m.mResets.Add(1)
		}
	}
	// A same-run sample whose every input was missing is still a baseline: nothing was derived,
	// and saying so is what keeps an empty rate distinguishable from a zero one.
	return !wrote
}

// sumDelta adds up the change in attrs between two observations. ok is false unless every input
// was present in BOTH, which is deliberate in two directions:
//
//   - An input that vanished: HTCondor DELETES a starter-sourced attribute from the job ad when
//     an update omits it (CopyAttribute), so a partial sum would be a wrong number rather than a
//     smaller one.
//   - An input that only just appeared: the tempting shortcut is to read "absent in the previous
//     observation" as zero, since HTCondor does zero the per-run counters at run start. It is not
//     worth it. The predecessor there is usually the run's spawn commit, and the interval from it
//     spans input file transfer -- time the job was not running -- so a rate measured across it
//     would be wrong (too low) even with a correct zero. And for a job the daemon first sees
//     mid-run, "absent" does not mean zero at all, and the invented baseline would show up as a
//     spike. The cost of refusing is one rate-less sample per run; the cost of guessing is a
//     fabricated number in a dashboard.
func sumDelta(prev, cur *prevObservation, attrs []string) (float64, bool) {
	var sum float64
	for _, a := range attrs {
		p, ok1 := prev.counters[a]
		c, ok2 := cur.counters[a]
		if !ok1 || !ok2 {
			return 0, false
		}
		sum += c - p
	}
	return sum, true
}

// inheritedCounters reports whether a new run's counters look like the previous run's, i.e. the
// shadow has not yet pushed its zeroing update. Diagnostic only.
func inheritedCounters(prev, cur *prevObservation) bool {
	p, ok1 := prev.counters["RemoteUserCpu"]
	c, ok2 := cur.counters["RemoteUserCpu"]
	return ok1 && ok2 && c > 0 && c >= p
}

// isExecuting reports whether a job status means the job is on an execute node accumulating
// usage: Running, Transferring Output, or Suspended (suspended still holds the slot, and its
// counters stand still -- which the interval arithmetic nets out rather than ignoring).
func isExecuting(status int64) bool {
	switch status {
	case 2, 6, 7:
		return true
	}
	return false
}

// runInstanceOf derives the run-attempt id the way HTCondor's epoch writer does, so a sample and
// an epoch_history record name the same run.
func runInstanceOf(job *classad.ClassAd) (int64, bool) {
	if n, ok := job.EvaluateAttrInt(numShadowStartsAttr); ok && n > 0 {
		return n - 1, true
	}
	return 0, false
}

// sampleTimeOf returns the observation instant and whether it came from the starter's clock.
func (m *jobMetrics) sampleTimeOf(job *classad.ClassAd) (int64, bool) {
	if t, ok := job.EvaluateAttrInt(statsClockAttr); ok && t > 0 {
		return t, true
	}
	return m.now().Unix(), false
}

// copyAttr copies one attribute if the job has it, preserving its literal value. Undefined and
// error values are skipped: an absent column is how a gap renders, and materializing one would
// turn "not reported" into data.
func copyAttr(dst, src *classad.ClassAd, name string) {
	v := src.EvaluateAttr(name)
	if v.IsUndefined() {
		return
	}
	switch {
	case v.IsInteger():
		if i, ok := src.EvaluateAttrInt(name); ok {
			_ = dst.Set(name, i)
		}
	case v.IsNumber():
		if f, ok := src.EvaluateAttrNumber(name); ok {
			_ = dst.Set(name, f)
		}
	case v.IsString():
		if s, ok := src.EvaluateAttrString(name); ok {
			_ = dst.Set(name, s)
		}
	default:
		if b, ok := src.EvaluateAttrBool(name); ok {
			_ = dst.Set(name, b)
		}
	}
}

// flush appends collected samples, skipping any the archive already holds while in recovery.
func (m *jobMetrics) flush(samples []*classad.ClassAd) {
	if m == nil || len(samples) == 0 {
		return
	}
	for _, s := range samples {
		if m.dedup {
			if m.alreadyHave(s) {
				m.mDeduped.Add(1)
				continue
			}
			// Past the replayed prefix: everything from here is new.
			m.dedup = false
		}
		if err := m.archive.Append(s); err != nil {
			m.mAppendFails.Add(1)
			m.log.Warn("job metrics: append failed", "err", err.Error())
			continue
		}
		m.mAppended.Add(1)
	}
}

// alreadyHave reports whether the archive already holds this observation. The constraint is the
// same "identity as a query, not as a key" approach the epoch tailer uses: an archive record has
// no key (collections.Archive appends with a nil key), so a record's identity is whatever set of
// attributes distinguishes it.
//
// (job, run, instant, trigger) is unique for every sample a replay can reproduce EXCEPT two
// commits of the same kind between two starter reports -- where the second would be dropped. That
// is a bounded loss confined to the few seconds of log a crash re-reads, and dropping a sample is
// a better error than double-counting one.
func (m *jobMetrics) alreadyHave(s *classad.ClassAd) bool {
	c, ok := dedupConstraint(s)
	if !ok {
		return false
	}
	seq, err := m.archive.QueryLimit(c, 1)
	if err != nil {
		// A malformed constraint must not stop sampling; treat it as "not present" (the failure
		// mode is a duplicate, not a gap) and say so once per occurrence.
		m.log.Warn("job metrics: dedup query failed", "err", err.Error())
		return false
	}
	for range seq {
		return true
	}
	return false
}

func dedupConstraint(s *classad.ClassAd) (string, bool) {
	cid, ok1 := s.EvaluateAttrInt("ClusterId")
	pid, ok2 := s.EvaluateAttrInt("ProcId")
	ts, ok3 := s.EvaluateAttrInt(SampleTimeAttr)
	if !ok1 || !ok2 || !ok3 {
		return "", false
	}
	trig, _ := s.EvaluateAttrString(SampleTriggerAttr)
	run, haveRun := s.EvaluateAttrInt(RunInstanceAttr)
	runTerm := fmt.Sprintf("%s is undefined", RunInstanceAttr)
	if haveRun {
		runTerm = fmt.Sprintf("%s == %d", RunInstanceAttr, run)
	}
	return fmt.Sprintf("ClusterId == %d && ProcId == %d && %s && %s == %d && %s == %s",
		cid, pid, runTerm, SampleTimeAttr, ts, SampleTriggerAttr, classadStringLit(trig)), true
}

// MetricsStatus is the sampler's counters, surfaced through the job syncer's SyncStatus so an
// operator sees sample volume and the two "our model of HTCondor is wrong" signals (Resets,
// InheritedCounters) next to the tailer's own health.
type MetricsStatus struct {
	Appended          int64 // samples written to the archive
	Throttled         int64 // samples dropped by MinInterval
	Baseline          int64 // samples with no derived rates (no usable predecessor)
	Resets            int64 // rate suppressed because a counter went backwards
	InheritedCounters int64 // new run still carrying the previous run's counters
	Deduped           int64 // samples skipped as already present (restart replay)
	AppendFailures    int64
}

func (m *jobMetrics) status() MetricsStatus {
	if m == nil {
		return MetricsStatus{}
	}
	return MetricsStatus{
		Appended:          m.mAppended.Load(),
		Throttled:         m.mThrottled.Load(),
		Baseline:          m.mBaseline.Load(),
		Resets:            m.mResets.Load(),
		InheritedCounters: m.mInherited.Load(),
		Deduped:           m.mDeduped.Load(),
		AppendFailures:    m.mAppendFails.Load(),
	}
}

// JobMetricsZoneAttrs / JobMetricsValueAttrs / JobMetricsCategoricalAttrs are the index set the
// table is created with. SampleTime must be zone-mapped both because every dashboard query filters
// on it (so a time range prunes whole segments instead of scanning) and because age-based
// retention measures against a zone-mapped attribute.
var (
	JobMetricsZoneAttrs        = []string{SampleTimeAttr, "ClusterId"}
	JobMetricsValueAttrs       = []string{"ClusterId"}
	JobMetricsCategoricalAttrs = []string{"Owner"}
)
