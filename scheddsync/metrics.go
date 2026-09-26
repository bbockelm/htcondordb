package scheddsync

import (
	"encoding/json"
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
	// only -- what suppresses a re-read is the flush position, not the record's contents (see
	// flush).
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
	//
	// CurrentResidentSetSize is the one number in this table that is a true memory GAUGE rather
	// than a ratchet -- the RSS the starter measured, before the high-water maxes are applied.
	// It does not exist in HTCondor yet (see the design doc's upstream asks); recording it here
	// costs nothing on a pool that does not publish it, because an absent attribute is simply not
	// written, and means a pool that gains it starts plotting real working-set curves with no
	// change here.
	"CurrentResidentSetSize":     classGauge,
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
	Out string
	In  []string
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
	{Out: "BytesSentRate", In: []string{"BytesSent"},
		Doc: "bytes/s sent to the execute node (input transfer)"},
	{Out: "BytesRecvdRate", In: []string{"BytesRecvd"},
		Doc: "bytes/s received from the execute node (output transfer)"},
}

// derivedRatio is a fraction of a resource's usage to its request. No predecessor is needed, so
// these are emitted on every sample including a baseline one.
//
// Scale converts the numerator into the denominator's units, and exists because the units in this
// table are NOT uniform -- MemoryUsage is MiB while ResidentSetSize is KiB, against a RequestMemory
// in MiB -- so a ratio built from the wrong pair is silently wrong by a factor of 1024 rather than
// visibly broken. Doing the conversion once here, next to the declaration, is the point.
type derivedRatio struct {
	Out, Num, Den string
	Scale         float64 // multiplies Num before dividing; 0 means 1
}

var derivedRatios = []derivedRatio{
	// Both MiB. This is a HIGH-WATER-MARK ratio (see the package comment): for RunInstanceID > 0
	// the numerator is the maximum over all of the job's runs, not this one's.
	{Out: "MemUtil", Num: "MemoryUsage", Den: "RequestMemory"},
	// KiB over MiB. The gauge counterpart of MemUtil, and the only one of the two that can go
	// down -- so it is the one to plot against a memory request over time.
	{Out: "CurrentMemUtil", Num: "CurrentResidentSetSize", Den: "RequestMemory", Scale: 1.0 / 1024},
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
		"vacatereason", "lastvacatetime":
		// Deliberately NOT CompletionDate: the schedd writes it as 0 at SUBMIT, so it is a
		// terminal signal only by its value, never by being set. JobStatus and ExitCode carry
		// the real transition.
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
	// fromStarter records WHICH CLOCK sampleTime came from. Differentiating a starter-clock
	// instant against an ingest-clock one measures the AP/EP skew as if it were elapsed time: with
	// the AP behind, the interval can even go negative and silently drop every rate in the window.
	// The two are not comparable, so an interval spanning both is refused.
	fromStarter bool
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
	// Store persists the position through which samples have been flushed, so a restart does not
	// re-append the window the tailer re-reads. Nil keeps the sampler in-memory only.
	Store  PositionStore
	Logger *slog.Logger
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

	// ended records, per job, the last RunInstanceID whose endpoint sample was already emitted.
	//
	// It exists because HTCondor starts a run in TWO transactions and bumps NumShadowStarts in the
	// SECOND one: Scheduler::start_std calls mark_serial_job_running() -- its own transaction,
	// setting JobStatus=2 and nothing else of interest -- before add_shadow_rec(), which is where
	// NumShadowStarts moves (schedd.cpp). So on the JobStatus=2 commit the row still reports the
	// id of the run that just ENDED, and without this the sampler emits an extra sample for that
	// finished run, after its own endpoint, carrying the previous run's counters.
	//
	// This is the same shape as the endpoint rule itself: a logical state change that HTCondor
	// spreads over several commits cannot be read from any one of them.
	ended map[string]int64

	// mark is the log position through which samples have been flushed, restored at startup from
	// store. Everything at or before it has already been written; see flush.
	mark  logPos
	store PositionStore

	lastAppendWarn time.Time
	warnedSave     bool

	mAppended    atomic.Int64
	mThrottled   atomic.Int64
	mAfterEnd    atomic.Int64
	mClockMix    atomic.Int64
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
		ended:       map[string]int64{},
		store:       cfg.Store,
	}
	m.restoreMark()
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
	delete(m.ended, key)
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
// mention.
//
// It is called BEFORE the commit, and BOTH its results -- the samples and the predecessor-cache
// updates -- are applied only once that commit has landed. Returning the cache updates rather
// than applying them here is the whole point: a sample that gets discarded must not move the
// cache it would have been measured from, and for a TERMINAL sample the update deletes the
// predecessor -- after which a re-applied pass finds none, does not recognise the endpoint, and
// emits nothing for it. The run would lose its endpoint permanently and silently.
//
// Each key appears at most once per transaction (pending is keyed by job), so no sample in a
// batch depends on another's cache update.
func (m *jobMetrics) collect(read rowReader, logSeq int64) ([]*classad.ClassAd, []prevUpdate) {
	if m == nil || len(m.pending) == 0 {
		return nil, nil
	}
	out := make([]*classad.ClassAd, 0, len(m.pending))
	var updates []prevUpdate
	for key, trig := range m.pending {
		ad, ok := read.LookupClassAd(key)
		if !ok || ad == nil {
			// The row is gone (destroyed in this same transaction). Nothing to sample.
			continue
		}
		if s := m.build(key, trig, ad, logSeq, &updates); s != nil {
			out = append(out, s)
		}
	}
	clear(m.pending)
	return out, updates
}

// commitPending applies the cache updates from a collect whose transaction actually committed.
func (m *jobMetrics) commitPending(updates []prevUpdate) {
	if m == nil {
		return
	}
	for _, u := range updates {
		if u.next == nil {
			delete(m.prev, u.key)
		} else {
			m.prev[u.key] = u.next
		}
		if u.haveEnded {
			m.ended[u.key] = u.endedRun
		}
	}
}

// rowReader is the read side of whatever holds the job rows -- a *db.Txn during a commit, a
// *db.DB in tests. Narrowed to the one method the sampler needs.
type rowReader interface {
	LookupClassAd(key string) (*classad.ClassAd, bool)
}

// build turns one job row into a sample, advancing the predecessor cache. Returns nil when the
// sample is throttled away.
func (m *jobMetrics) build(key string, trig sampleTrigger, job *classad.ClassAd, logSeq int64, pending *[]prevUpdate) *classad.ClassAd {
	run, haveRun := runInstanceOf(job)
	sampleTime, fromStarter := m.sampleTimeOf(job)
	prev := m.prev[key]
	status, _ := job.EvaluateAttrInt("JobStatus")

	// What counts as the end of a run, and why it is not "the commit that set ExitCode".
	//
	// The schedd spreads a completion across SEVERAL transactions, and the one carrying the
	// terminal-looking attributes is NOT the one that changes the status. Observed against a real
	// schedd, in order: a transaction setting ExitCode/ExitBySignal/MemoryUsage while JobStatus is
	// still 2; then more transactions of transfer timings and committed time; then, separately,
	// the one that sets JobStatus to 4. So no single commit is both terminal-looking and
	// terminally-statused, and a rule requiring both emits no endpoint at all.
	//
	// The reliable signal is the TRANSITION: this tailer was sampling the run (it holds a
	// predecessor for it), and the job is no longer executing. That is the run's last sample
	// whatever attribute happened to trigger the commit, and by then the row has accumulated the
	// final numbers the earlier transactions wrote.
	wasSampling := prev != nil
	if done, ok := m.ended[key]; ok && run <= done {
		// This run's endpoint has already been emitted. Anything still arriving under its id is
		// either the pre-bump window of the NEXT run (see the `ended` field) or late bookkeeping
		// on a finished one; neither is an observation of it.
		m.mAfterEnd.Add(1)
		return nil
	}
	switch {
	case !haveRun:
		return nil // never ran; nothing to observe
	case isExecuting(status):
		// An attribute name is a hint about a commit, not a verdict on the job: a vacate time
		// from an earlier run, or a hold reason being cleared, can ride along with ordinary
		// updates. Labelling that "terminal" would put an endpoint mid-run, which is worse than
		// cosmetic -- a consumer filtering to terminal samples relies on there being one per run.
		if trig == triggerTerminal {
			trig = triggerStatus
		}
	case wasSampling:
		// The run ended (completed, evicted back to idle, or held). Requires a predecessor
		// DELIBERATELY: a terminal-looking attribute alone is not evidence a run ended here.
		// condor_hold and condor_rm set HoldReason/RemoveReason together with JobStatus in one
		// transaction, and they do that for IDLE jobs too -- a job that ran earlier still carries
		// NumShadowStarts, so without this a held-while-idle job produced a second "terminal"
		// record for a run that had already ended, at the same SampleTime, with the old run's
		// counters. A run whose samples we never saw has no endpoint to record.
		trig = triggerTerminal
	default:
		// Not executing and not a run we were following: schedd bookkeeping on an idle or held
		// job, which is not an observation of resource usage.
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

	// The storage key, always. ClusterId reaches a proc row only through cluster-ad chaining, so a
	// row that never got chained yields a sample with no ClusterId and no GlobalJobId -- with no
	// way to tell whose it is, and no key to repair it by in an append-only table. It is in hand
	// here for free.
	_ = s.Set(KeyAttr, key)
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
			scale := r.Scale
			if scale == 0 {
				scale = 1
			}
			_ = s.Set(r.Out, num*scale/den)
		}
	}

	cur := m.observe(job, run, sampleTime, fromStarter)
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

	// The caller applies this only if the transaction commits -- see collect. Advancing the cache
	// here would survive a failed commit whose sample was discarded, and for a terminal sample it
	// would DELETE the predecessor, after which the re-applied pass finds wasSampling false and
	// emits no endpoint at all. The run would silently lose it.
	upd := prevUpdate{key: key, next: cur}
	if trig == triggerTerminal {
		upd.next = nil // the run ended; the next one starts a fresh series
		upd.endedRun, upd.haveEnded = run, true
	}
	*pending = append(*pending, upd)
	return s
}

// prevUpdate is one deferred change to the predecessor cache, applied by commitPending once the
// transaction the sample came from has actually committed.
type prevUpdate struct {
	key       string
	next      *prevObservation // nil deletes the entry (the run ended)
	endedRun  int64
	haveEnded bool
}

// observe snapshots the counters this sample will be differentiated from next time.
func (m *jobMetrics) observe(job *classad.ClassAd, run, sampleTime int64, fromStarter bool) *prevObservation {
	o := &prevObservation{run: run, sampleTime: sampleTime, fromStarter: fromStarter,
		counters: map[string]float64{}}
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
	// No rate spans a run boundary, including the counters that accumulate across runs.
	//
	// Those used to derive one, on the reasoning that their inputs do not restart so the delta is
	// sound. The delta is -- but the DENOMINATOR is not: the interval between a run's last sample
	// and the next run's first spans however long the job sat idle in between. A job that
	// transferred 1 MB in 100s, six hours after its previous run ended, recorded 46 B/s and was
	// not marked baseline, so nothing distinguished it from a genuinely slow transfer. A rate
	// averaged over time the job was not running is not a rate.
	if prev.run != run {
		return true
	}
	// An interval may not span two different clocks. sampleTime is the starter's when it reports
	// stats and the ingest clock otherwise, and the gap between them is AP/EP skew, not elapsed
	// time. With the AP behind, the difference even goes negative and silently drops every rate
	// in the window; ahead, every rate is understated by the skew with nothing on the record.
	if prev.fromStarter != cur.fromStarter {
		m.mClockMix.Add(1)
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

	wrote := false
	for _, r := range derivedRates {
		delta, ok := sumDelta(m, prev, cur, r.In)
		if !ok {
			continue
		}
		_ = s.Set(r.Out, delta/interval)
		wrote = true
	}
	// GPU is a LIFETIME average, not a counter: GPUsAverageUsage is built by the startd as
	// (Uptime - StartOfJobUptime)/(LastUpdate - FirstUpdate). Recovering the per-interval value
	// means un-averaging it over the two elapsed times.
	if cur.hasGPU && prev.hasGPU && cur.elapsed > 0 && prev.elapsed > 0 {
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
func sumDelta(m *jobMetrics, prev, cur *prevObservation, attrs []string) (float64, bool) {
	var sum float64
	for _, a := range attrs {
		p, ok1 := prev.counters[a]
		c, ok2 := cur.counters[a]
		if !ok1 || !ok2 {
			return 0, false
		}
		// Each input is guarded SEPARATELY. Guarding only the sum lets one input's reset hide
		// inside another's progress: RemoteUserCpu 100->160 with RemoteSysCpu 50->0 sums to +10,
		// which is positive, so it was written as a rate of 0.1 cores against a true value of at
		// least 0.6 -- neither suppressed nor counted, which is precisely what the reset counter
		// exists to make visible.
		d := c - p
		if d < 0 {
			// A counter went backwards: a reset we did not predict, or the starter stopped
			// reporting and started again. Emit no rate rather than a wrong one, and count it --
			// a climbing value means the reset table in this file is missing a case.
			m.mResets.Add(1)
			return 0, false
		}
		sum += d
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
// logPos is where in the job_queue.log stream a batch of samples came from: the log generation
// (the schedd's op-107 sequence number, bumped on every compaction) and the byte offset just past
// the entry that closed the transaction. Offsets are absolute within a generation, so the same
// entry yields the same position however far back the pass began -- which is what lets a position
// be compared against a durable mark.
type logPos struct {
	Seq    int64 `json:"seq"`
	Offset int64 `json:"offset"`
}

// after reports whether p is strictly past mark, i.e. not already flushed. A different generation
// is never comparable -- a compaction resets offsets -- so it counts as new.
func (p logPos) after(mark logPos) bool {
	if p.Seq != mark.Seq {
		return true
	}
	return p.Offset > mark.Offset
}

// flush appends the samples from one committed transaction, unless that transaction's position has
// already been flushed.
//
// WHY A POSITION AND NOT THE RECORD'S CONTENTS. Appends are not idempotent, and the log is re-read
// in two ordinary situations: a commit conflict rewinds the pass to the last durable offset, and a
// restart resumes from it. The first design recognised an already-written sample by querying the
// archive for one with the same (job, run, SampleTime, trigger). That cannot work, because
// SampleTime falls back to the INGEST CLOCK whenever the job carries no starter stats -- which is
// most samples, since a status commit carries none -- so the replayed sample simply had a
// different timestamp, matched nothing, and was appended again. Worse, the check switched itself
// off after one miss, so that single unmatched sample disabled suppression for the whole replayed
// window.
//
// The position has none of those properties: it is assigned by the log, not by us, and it
// reproduces exactly.
//
// The mark is persisted BEFORE the append, so a crash in between loses samples rather than
// duplicating them -- the same direction the rest of this table prefers, since a gap renders as a
// gap while a duplicate silently distorts an average.
func (m *jobMetrics) flush(samples []*classad.ClassAd, pos logPos) {
	if m == nil || len(samples) == 0 {
		return
	}
	if !pos.after(m.mark) {
		// Already flushed: a rewind or a restart is re-reading this region.
		m.mDeduped.Add(int64(len(samples)))
		return
	}
	m.mark = pos
	m.saveMark()
	for _, s := range samples {
		if err := m.archive.Append(s); err != nil {
			m.mAppendFails.Add(1)
			m.logAppendFailure(err)
			continue
		}
		m.mAppended.Add(1)
	}
}

// saveMark persists the flush mark. A nil store leaves the sampler in-memory only, which is what
// tests and an unconfigured deployment get; the cost of losing it is a replayed window's worth of
// duplicates after a restart, not corruption.
func (m *jobMetrics) saveMark() {
	if m.store == nil {
		return
	}
	blob, err := json.Marshal(m.mark)
	if err != nil {
		return
	}
	if serr := m.store.Save(blob); serr != nil && !m.warnedSave {
		m.warnedSave = true
		m.log.Warn("job metrics: saving the flush position failed; a restart may duplicate "+
			"samples for the replayed window", "err", serr.Error())
	}
}

// restoreMark loads the persisted flush position. Absent or unreadable leaves it zero, which
// treats everything as new -- the safe direction for a first run, and for a restart the cost is
// bounded by how much log the tailer re-reads.
func (m *jobMetrics) restoreMark() {
	if m.store == nil {
		return
	}
	blob, ok, err := m.store.Load()
	if err != nil || !ok {
		return
	}
	var mark logPos
	if json.Unmarshal(blob, &mark) == nil {
		m.mark = mark
	}
}

// resetMark forgets the flush position. Called when the log is replaced under us (a compaction
// the sequence number did not distinguish), since offsets in the new file mean nothing against a
// mark from the old one -- and comparing them would suppress real samples rather than duplicates.
func (m *jobMetrics) resetMark() {
	if m == nil {
		return
	}
	m.mark = logPos{}
	m.saveMark()
}

// logAppendFailure rate-limits the append warning. A persistently failing archive (a full disk)
// otherwise produces one line per sample per poll, which makes a disk problem worse.
func (m *jobMetrics) logAppendFailure(err error) {
	now := m.now()
	if now.Sub(m.lastAppendWarn) < time.Minute {
		return
	}
	m.lastAppendWarn = now
	m.log.Warn("job metrics: append failed", "err", err.Error(),
		"failures_total", m.mAppendFails.Load())
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
	// AfterEnd counts commits arriving under a RunInstanceID whose endpoint was already emitted.
	// A steady trickle is normal: HTCondor bumps NumShadowStarts one transaction AFTER it sets
	// JobStatus=2, so every re-run has a brief window still reporting the previous run's id.
	AfterEnd int64
	// ClockMix counts rates suppressed because the interval would have spanned the starter's
	// clock and the ingest clock. Nonzero means starter stats are intermittent on this pool.
	ClockMix int64
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
		AfterEnd:          m.mAfterEnd.Load(),
		ClockMix:          m.mClockMix.Load(),
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
