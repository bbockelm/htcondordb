package scheddsync

// logwriter.go reconstructs a schedd job_queue.log (ClassAdLog format) from the
// mirrored DB tables -- the inverse of JobSync. It is the "out" half of a
// job_queue.log -> htcondordb -> job_queue.log round-trip: given the tables JobSync
// populates, it emits a COMPACTED log (one NewClassAd + one SetAttribute per
// attribute for each current ad, with no operation history) that reproduces the
// current queue state when replayed by a schedd or by JobSync.
//
// Fidelity notes (see the round-trip test):
//   - The header ad ("0.0", queue counters like NextClusterNum) is reconstructed when a
//     Header table is supplied -- this is what makes the output a queue-counter-preserving
//     backup rather than only a state reconstruction. Cluster-private ads and OCU ads are
//     still not stored by JobSync, so they are not reconstructed.
//   - Emission order is header, users, jobsets, clusters, then jobs (procs). Emitting every
//     cluster ad's attributes BEFORE any proc NewClassAd is what keeps a replay
//     correct: a cluster SetAttribute fans out onto its already-materialized proc
//     rows, so if a proc overrode a cluster attribute, a late cluster set would clobber
//     the override. With all cluster ops first, the cluster has no children yet when its
//     attributes are set, and each proc's own attributes (emitted after it chains from
//     the cluster) win -- matching how JobSync ingested them originally.

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
)

// QueueLogWriter reconstructs a job_queue.log from the tables JobSync mirrors into.
// Jobs is required; the sibling namespace tables are optional (a nil table contributes
// no records).
type QueueLogWriter struct {
	Jobs     *db.DB // proc ads ("C.P") -- required
	Users    *db.DB // owner/user records ("0.P")
	Jobsets  *db.DB // jobset ads ("C.-100")
	Clusters *db.DB // cluster ads ("0C.-1")
	Header   *db.DB // schedd header ad ("0.0", queue counters)
}

// WriteFile reconstructs the log and writes it to path (truncating any existing file).
func (w *QueueLogWriter) WriteFile(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if _, err := w.WriteTo(f); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// WriteTo reconstructs the log and writes it to out, returning the number of bytes written
// (satisfying io.WriterTo). The whole queue is emitted inside a single transaction
// (105...106) so a replay applies it atomically -- and so a JobSync replaying it never
// observes a partially-restored queue.
func (w *QueueLogWriter) WriteTo(out io.Writer) (int64, error) {
	if w.Jobs == nil {
		return 0, fmt.Errorf("scheddsync: QueueLogWriter requires a Jobs table")
	}
	cw := &countingWriter{w: out}
	bw := bufio.NewWriter(cw)
	if _, err := bw.WriteString("105\n"); err != nil { // BeginTransaction
		return cw.n, err
	}
	// Order matters: the header first (as in a real log), then all non-proc namespaces (in
	// particular every cluster ad) before any proc ad, so a cluster SetAttribute never fans out
	// onto an already-materialized proc.
	for _, table := range []*db.DB{w.Header, w.Users, w.Jobsets, w.Clusters, w.Jobs} {
		if err := writeTable(bw, table); err != nil {
			return cw.n, err
		}
	}
	if _, err := bw.WriteString("106\n"); err != nil { // EndTransaction
		return cw.n, err
	}
	if err := bw.Flush(); err != nil {
		return cw.n, err
	}
	return cw.n, nil
}

// countingWriter tallies bytes written so WriteTo can report a count.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// writeTable emits every ad in a table in ascending key order (deterministic output).
func writeTable(bw *bufio.Writer, table *db.DB) error {
	if table == nil {
		return nil
	}
	keys := table.Keys()
	sort.Strings(keys)
	for _, key := range keys {
		ad, ok := table.LookupClassAd(key)
		if !ok {
			continue
		}
		if err := writeAd(bw, key, ad); err != nil {
			return err
		}
	}
	return nil
}

// writeAd emits one ad as a NewClassAd (101) carrying MyType/TargetType, followed by a
// SetAttribute (103) per remaining attribute. MyType/TargetType ride the 101 line (JobSync
// re-inserts them there), so they are skipped in the attribute loop.
func writeAd(bw *bufio.Writer, key string, ad *classad.ClassAd) error {
	myType, _ := ad.EvaluateAttrString("MyType")
	targetType, _ := ad.EvaluateAttrString("TargetType")
	if _, err := bw.WriteString(newClassAdLine(key, myType, targetType)); err != nil {
		return err
	}
	attrs := ad.GetAttributes()
	sort.Strings(attrs)
	for _, name := range attrs {
		if name == "MyType" || name == "TargetType" {
			continue
		}
		expr, ok := ad.Lookup(name)
		if !ok {
			continue
		}
		val := expr.String()
		// The log is line-oriented (the parser joins fields with spaces and splits on
		// newlines); a value with an embedded newline would be torn across records. The
		// schedd escapes these -- the prototype refuses them loudly rather than corrupt.
		if strings.ContainsAny(val, "\n\r") {
			return fmt.Errorf("scheddsync: attribute %s on %s has a multiline value; the prototype log writer does not escape these", name, key)
		}
		if _, err := fmt.Fprintf(bw, "103 %s %s %s\n", key, name, val); err != nil {
			return err
		}
	}
	return nil
}

// newClassAdLine builds a "101 <key> [<MyType> [<TargetType>]]" line. TargetType is
// positional (field 4), so it is only emitted when MyType (field 3) is present.
func newClassAdLine(key, myType, targetType string) string {
	if myType == "" {
		return "101 " + key + "\n"
	}
	if targetType == "" {
		return "101 " + key + " " + myType + "\n"
	}
	return "101 " + key + " " + myType + " " + targetType + "\n"
}
