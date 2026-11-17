package server

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// manifestSuffix is appended to a snapshot's name to form its sidecar manifest. The manifest is a
// separate file rather than a header or trailer inside the .cadb so that the snapshot format itself
// is unchanged: a snapshot written by this version restores on an older daemon, and a snapshot
// written by an older one verifies here as "no manifest" rather than as damage.
const manifestSuffix = ".manifest"

// manifestFormat is the manifest's own version, independent of the snapshot format.
const manifestFormat = 1

// Manifest describes one snapshot file: enough to check that the bytes on disk are the bytes that
// were written, and to see what the snapshot does and does not contain without opening it.
//
// Tables and SkippedArchives together answer the question the file cannot: a .cadb holds the
// catalog's mutable tables only, so an operator looking at a snapshot of a schedd-sync deployment
// needs to be told, in the artifact itself, that the archives were not in it.
type Manifest struct {
	Format          int       // manifestFormat
	File            string    // base name of the snapshot this describes
	Created         time.Time // when the snapshot finished being written (UTC)
	Bytes           int64     // size of the snapshot file
	SHA256          string    // hex digest of the whole snapshot file
	Duration        time.Duration
	Tables          []string // mutable tables included in the snapshot
	SkippedArchives []string // archive tables NOT included -- back these up separately
}

// manifestPath is the sidecar path for a snapshot.
func manifestPath(snapshot string) string { return snapshot + manifestSuffix }

// Encode renders the manifest as old-ClassAd text, the same shape the rest of the daemon uses for
// out-of-band records.
func (m *Manifest) Encode(w io.Writer) error {
	list := func(ss []string) string {
		q := make([]string, len(ss))
		for i, s := range ss {
			q[i] = strconv.Quote(s)
		}
		return "{ " + strings.Join(q, ", ") + " }"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Format = %d\n", m.Format)
	fmt.Fprintf(&b, "File = %s\n", strconv.Quote(m.File))
	fmt.Fprintf(&b, "Created = %s\n", strconv.Quote(m.Created.UTC().Format(time.RFC3339)))
	fmt.Fprintf(&b, "Bytes = %d\n", m.Bytes)
	fmt.Fprintf(&b, "SHA256 = %s\n", strconv.Quote(m.SHA256))
	fmt.Fprintf(&b, "DurationSeconds = %.3f\n", m.Duration.Seconds())
	fmt.Fprintf(&b, "Tables = %s\n", list(m.Tables))
	fmt.Fprintf(&b, "SkippedArchives = %s\n", list(m.SkippedArchives))
	_, err := io.WriteString(w, b.String())
	return err
}

// writeManifest writes m beside its snapshot, atomically (temp file + rename), and fsyncs it. The
// snapshot is already durable by the time this runs, so a crash here loses the manifest and not the
// backup -- which verify reports as "no manifest", the same as a snapshot from an older daemon.
func writeManifest(snapshot string, m *Manifest) error {
	path := manifestPath(snapshot)
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if err := m.Encode(f); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

// ReadManifest loads the sidecar manifest for a snapshot. A missing manifest returns os.ErrNotExist
// so callers can distinguish "written before manifests existed" from "unreadable".
func ReadManifest(snapshot string) (*Manifest, error) {
	f, err := os.Open(manifestPath(snapshot))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	m := &Manifest{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		name, val, ok := strings.Cut(sc.Text(), "=")
		if !ok {
			continue
		}
		name, val = strings.TrimSpace(name), strings.TrimSpace(val)
		unquote := func(s string) string {
			if u, err := strconv.Unquote(s); err == nil {
				return u
			}
			return s
		}
		switch name {
		case "Format":
			m.Format, _ = strconv.Atoi(val)
		case "File":
			m.File = unquote(val)
		case "Created":
			m.Created, _ = time.Parse(time.RFC3339, unquote(val))
		case "Bytes":
			m.Bytes, _ = strconv.ParseInt(val, 10, 64)
		case "SHA256":
			m.SHA256 = unquote(val)
		case "DurationSeconds":
			if secs, err := strconv.ParseFloat(val, 64); err == nil {
				m.Duration = time.Duration(secs * float64(time.Second))
			}
		case "Tables":
			m.Tables = parseAdList(val, unquote)
		case "SkippedArchives":
			m.SkippedArchives = parseAdList(val, unquote)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if m.Format == 0 {
		return nil, fmt.Errorf("server: %s is not a snapshot manifest", manifestPath(snapshot))
	}
	return m, nil
}

// parseAdList pulls the elements out of an old-ClassAd list literal: { "a", "b" }.
func parseAdList(s string, unquote func(string) string) []string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "{")
	s = strings.TrimSuffix(s, "}")
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, unquote(part))
		}
	}
	return out
}

// hashFile returns the sha256 of a file's contents and its size.
func hashFile(path string) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, bufio.NewReaderSize(f, 1<<20))
	if err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// VerifyResult is the outcome of checking one snapshot against its manifest.
type VerifyResult struct {
	File        string
	Ok          bool
	HasManifest bool
	Bytes       int64
	WantBytes   int64
	SHA256      string
	WantSHA256  string
	Problem     string // empty when Ok
	Manifest    *Manifest
}

// VerifySnapshot re-reads a snapshot and checks it against its sidecar manifest: size first (cheap,
// and it catches a truncated copy immediately) and then the whole-file digest.
//
// A snapshot with no manifest is reported with Ok false and HasManifest false, never as damage --
// snapshots written before manifests existed are intact, they simply cannot be checked this way.
func VerifySnapshot(path string) (VerifyResult, error) {
	res := VerifyResult{File: path}
	st, err := os.Stat(path)
	if err != nil {
		return res, err
	}
	res.Bytes = st.Size()

	m, err := ReadManifest(path)
	if err != nil {
		if os.IsNotExist(err) {
			res.Problem = "no manifest beside this snapshot; it cannot be verified (written before manifests, or the manifest was lost)"
			return res, nil
		}
		return res, err
	}
	res.HasManifest, res.Manifest = true, m
	res.WantBytes, res.WantSHA256 = m.Bytes, m.SHA256

	if res.Bytes != m.Bytes {
		res.Problem = fmt.Sprintf("size is %d bytes, manifest says %d -- the file is truncated or was modified", res.Bytes, m.Bytes)
		return res, nil
	}
	sum, _, err := hashFile(path)
	if err != nil {
		return res, err
	}
	res.SHA256 = sum
	if sum != m.SHA256 {
		res.Problem = "checksum mismatch: the file's contents are not what was written"
		return res, nil
	}
	res.Ok = true
	return res, nil
}

// VerifyDir verifies every snapshot in dir, newest first.
func VerifyDir(dir string) ([]VerifyResult, error) {
	names, err := snapshotNames(dir)
	if err != nil {
		return nil, err
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	out := make([]VerifyResult, 0, len(names))
	for _, n := range names {
		res, err := VerifySnapshot(dir + string(os.PathSeparator) + n)
		if err != nil {
			res.Problem = err.Error()
		}
		out = append(out, res)
	}
	return out, nil
}

// snapshotNames lists the snapshot files in dir (not their manifests).
func snapshotNames(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		n := e.Name()
		if !e.IsDir() && strings.HasPrefix(n, snapshotPrefix) && strings.HasSuffix(n, snapshotSuffix) {
			names = append(names, n)
		}
	}
	return names, nil
}

// BackupStats is what the daemon knows about its own backups, for the metrics endpoint. It is a
// snapshot of counters, safe to read while backups run.
type BackupStats struct {
	Total        int64     // snapshots attempted
	Failures     int64     // snapshots that returned an error
	LastSuccess  time.Time // zero if none since start
	LastFailure  time.Time // zero if none since start
	LastBytes    int64     // size of the most recent successful snapshot
	LastDuration time.Duration
	LastError    string
}

// backupStats accumulates BackupStats. A single mutex is enough: snapshots are minutes apart and a
// scrape is cheap.
type backupStatsRecorder struct {
	mu sync.Mutex
	s  BackupStats
}

func (r *backupStatsRecorder) success(bytes int64, d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.s.Total++
	r.s.LastSuccess = time.Now().UTC()
	r.s.LastBytes = bytes
	r.s.LastDuration = d
	r.s.LastError = ""
}

func (r *backupStatsRecorder) failure(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.s.Total++
	r.s.Failures++
	r.s.LastFailure = time.Now().UTC()
	r.s.LastError = err.Error()
}

func (r *backupStatsRecorder) get() BackupStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.s
}

// BackupStats reports this daemon's backup history since start. Wire it to the metrics endpoint.
func (s *Service) BackupStats() BackupStats { return s.backups.get() }
