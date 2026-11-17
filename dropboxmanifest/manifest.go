// Package dropboxmanifest describes one batch in an archive dropbox: how many records it holds,
// what it covers, and a digest of its bytes.
//
// A dropbox without manifests can be checked only for what it visibly lacks. The restore can see
// a hole in the sequence numbers between the batches present, but it cannot tell whether a batch
// it read whole was read whole: a damaged batch simply yields fewer records, and nothing knows
// how many there should have been. That is the gap this closes. A manifest states the record
// count, so a short read is an assertion failure instead of a quiet shortfall.
//
// It is a sidecar, not a header inside the batch, for the same reason the snapshot manifest is:
// the batch format stays exactly what an external consumer already reads, and a batch written
// before manifests existed is unverifiable rather than damaged.
package dropboxmanifest

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// Suffix is appended to a batch's path to form its manifest path.
const Suffix = ".manifest"

// Format is the manifest's own version.
const Format = 1

// Manifest describes one batch file.
type Manifest struct {
	Format    int
	File      string // base name of the batch this describes
	Seq       uint64 // this batch's sequence number
	PrevSeq   uint64 // the sequence number of the batch before it; 0 if this is the first
	Records   int    // how many records the batch holds
	FirstTime int64  // record-time of the first record (0 if unknown)
	LastTime  int64  // record-time of the last record (0 if unknown)
	Bytes     int64  // size of the batch file
	SHA256    string // hex digest of the batch file
	Container string // "targz" or "frames"
	Exporter  string // the exporter that wrote it
	Table     string // the archive it came from
}

// Path returns the manifest path for a batch.
func Path(batch string) string { return batch + Suffix }

// Encode renders the manifest as old-ClassAd text, the shape the rest of the dropbox uses.
func (m *Manifest) Encode(w io.Writer) error {
	var b strings.Builder
	fmt.Fprintf(&b, "MyType = \"ArchiveDropboxBatch\"\n")
	fmt.Fprintf(&b, "Format = %d\n", m.Format)
	fmt.Fprintf(&b, "File = %s\n", strconv.Quote(m.File))
	fmt.Fprintf(&b, "Seq = %d\n", m.Seq)
	fmt.Fprintf(&b, "PrevSeq = %d\n", m.PrevSeq)
	fmt.Fprintf(&b, "Records = %d\n", m.Records)
	fmt.Fprintf(&b, "FirstRecordTime = %d\n", m.FirstTime)
	fmt.Fprintf(&b, "LastRecordTime = %d\n", m.LastTime)
	fmt.Fprintf(&b, "Bytes = %d\n", m.Bytes)
	fmt.Fprintf(&b, "SHA256 = %s\n", strconv.Quote(m.SHA256))
	fmt.Fprintf(&b, "Container = %s\n", strconv.Quote(m.Container))
	fmt.Fprintf(&b, "Exporter = %s\n", strconv.Quote(m.Exporter))
	fmt.Fprintf(&b, "Table = %s\n", strconv.Quote(m.Table))
	_, err := io.WriteString(w, b.String())
	return err
}

// Read loads the manifest beside a batch. A missing manifest returns an error satisfying
// os.IsNotExist, so callers can tell "written before manifests" from "unreadable".
func Read(batch string) (*Manifest, error) {
	f, err := os.Open(Path(batch))
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
		unq := func(s string) string {
			if u, err := strconv.Unquote(s); err == nil {
				return u
			}
			return s
		}
		switch name {
		case "Format":
			m.Format, _ = strconv.Atoi(val)
		case "File":
			m.File = unq(val)
		case "Seq":
			m.Seq, _ = strconv.ParseUint(val, 10, 64)
		case "PrevSeq":
			m.PrevSeq, _ = strconv.ParseUint(val, 10, 64)
		case "Records":
			m.Records, _ = strconv.Atoi(val)
		case "FirstRecordTime":
			m.FirstTime, _ = strconv.ParseInt(val, 10, 64)
		case "LastRecordTime":
			m.LastTime, _ = strconv.ParseInt(val, 10, 64)
		case "Bytes":
			m.Bytes, _ = strconv.ParseInt(val, 10, 64)
		case "SHA256":
			m.SHA256 = unq(val)
		case "Container":
			m.Container = unq(val)
		case "Exporter":
			m.Exporter = unq(val)
		case "Table":
			m.Table = unq(val)
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if m.Format == 0 {
		return nil, fmt.Errorf("dropboxmanifest: %s is not a batch manifest", Path(batch))
	}
	return m, nil
}

// Write stores a manifest beside its batch: temp file, fsync, atomic rename. fsyncDir, if
// non-nil, is called afterwards so the rename itself is durable.
//
// It is called only after the batch it describes is already durable and in place, so a failure
// here costs the manifest and not the backup.
func Write(batch string, m *Manifest, fsyncDir func(string) error) error {
	path := Path(batch)
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if err := m.Encode(f); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	if fsyncDir != nil {
		dir := path[:strings.LastIndexByte(path, os.PathSeparator)]
		return fsyncDir(dir)
	}
	return nil
}

// HashFile returns the sha256 and size of a file.
func HashFile(path string) (string, int64, error) {
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
