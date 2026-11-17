// Package dropboxrestore reads an archive dropbox back into an archive table.
//
// The dropbox exporter (the archivedropbox module) writes an archive's records out as
// self-contained old-ClassAd text so that no record depends on anything outside itself -- no
// shared compression dictionary, no interning, no cross-record structure. That is what makes a
// dropbox the one archive copy a damaged store cannot strand. This package is the other half:
// turning that pile of batches back into an archive.
//
// It lives in the main module, not in archivedropbox, because reading the dropbox needs nothing
// from the exporter -- only its on-disk format -- while writing to the database needs the daemon's
// client. Keeping it here leaves the exporter a standalone binary outside the core daemon build.
package dropboxrestore

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/PelicanPlatform/classad/classad"

	"github.com/bbockelm/htcondordb/dropboxframe"
	"github.com/bbockelm/htcondordb/dropboxmanifest"
)

// batchPattern matches the exporter's finished batches in either container:
// batch-<seq>-<firstModUnix>.tar.gz or .cadf. A dot-prefixed file is one the exporter is still
// writing and is deliberately not matched.
var batchPattern = regexp.MustCompile(`^batch-(\d+)-(-?\d+)\.(tar\.gz|cadf)$`)

// lossPattern matches the exporter's data-loss reports: loss-<detectedUnix>.ad.
var lossPattern = regexp.MustCompile(`^loss-(-?\d+)\.ad$`)

// Container is how a batch is packaged on disk.
type Container string

const (
	// ContainerTarGz is one gzip-compressed tar per batch.
	ContainerTarGz Container = "targz"
	// ContainerFrames is a framed zstd stream: damage costs one frame, not the batch.
	ContainerFrames Container = "frames"
)

// Batch is one batch file in a dropbox.
type Batch struct {
	Path      string
	Seq       uint64
	Mod       int64
	Container Container
	// Manifest is the batch's sidecar, nil when it has none (written before manifests existed,
	// or the manifest was lost). Without it a batch can be read but not checked.
	Manifest *dropboxmanifest.Manifest
}

// LossReport is one loss ClassAd the exporter dropped when retention outran its cursor. These are
// the windows the backup never held: an operator restoring from a dropbox needs them surfaced,
// because nothing else will tell them records are missing.
type LossReport struct {
	Path     string
	Detected int64
	Ad       *classad.ClassAd
}

// Listing is what a dropbox directory holds.
type Listing struct {
	Batches []Batch      // ordered by sequence
	Losses  []LossReport // ordered by detection time
	// MissingSeqs are sequence numbers absent between the lowest and highest present. A gap means
	// batches were deleted, never delivered, or lost -- it is not recoverable from here, but it is
	// knowable, and silently restoring an incomplete dropbox is the failure this reports.
	MissingSeqs []uint64
	// Partial are dot-prefixed temp files found in the directory: either the exporter is running
	// right now, or it died mid-write. Either way they are not data.
	Partial []string
}

// List enumerates a dropbox directory without reading the batches.
func List(dir string) (*Listing, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	l := &Listing{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			continue
		}
		if strings.HasPrefix(name, ".") {
			l.Partial = append(l.Partial, name)
			continue
		}
		if strings.HasSuffix(name, dropboxmanifest.Suffix) {
			continue // a batch's sidecar, read with its batch
		}
		if m := batchPattern.FindStringSubmatch(name); m != nil {
			seq, _ := strconv.ParseUint(m[1], 10, 64)
			mod, _ := strconv.ParseInt(m[2], 10, 64)
			cont := ContainerTarGz
			if m[3] == "cadf" {
				cont = ContainerFrames
			}
			b := Batch{Path: filepath.Join(dir, name), Seq: seq, Mod: mod, Container: cont}
			if m, err := dropboxmanifest.Read(b.Path); err == nil {
				b.Manifest = m
			}
			l.Batches = append(l.Batches, b)
			continue
		}
		if m := lossPattern.FindStringSubmatch(name); m != nil {
			det, _ := strconv.ParseInt(m[1], 10, 64)
			lr := LossReport{Path: filepath.Join(dir, name), Detected: det}
			if b, err := os.ReadFile(lr.Path); err == nil {
				lr.Ad, _ = classad.ParseOld(string(b))
			}
			l.Losses = append(l.Losses, lr)
		}
	}
	sort.Slice(l.Batches, func(i, j int) bool { return l.Batches[i].Seq < l.Batches[j].Seq })
	sort.Slice(l.Losses, func(i, j int) bool { return l.Losses[i].Detected < l.Losses[j].Detected })
	l.MissingSeqs = missingSeqs(l.Batches)
	return l, nil
}

// missingSeqs reports the sequence numbers absent between the lowest and highest batch present.
//
// Nothing outside that range is knowable from the sequence numbers alone, and that is not a
// defect: a consumer drains a dropbox from the front, so a low batch being gone is the normal
// case rather than a loss. What IS knowable is a hole between two batches that are still here,
// which is what this reports; the manifests confirm it by naming each batch's predecessor.
func missingSeqs(bs []Batch) []uint64 {
	if len(bs) < 2 {
		return nil
	}
	have := make(map[uint64]bool, len(bs))
	for _, b := range bs {
		have[b.Seq] = true
	}
	var out []uint64
	for s := bs[0].Seq; s <= bs[len(bs)-1].Seq; s++ {
		if !have[s] {
			out = append(out, s)
		}
	}
	return out
}

// Record is one record read out of a batch.
type Record struct {
	Batch Batch
	Entry string // the tar entry name it came from
	Ad    *classad.ClassAd
}

// ReadBatch decodes every record in one batch, in whichever container it is written.
//
// A damaged batch returns the records it managed to read along with the error, rather than
// nothing. For a tarball that matters a great deal: gzip is a single stream, so damage costs the
// tail from the damage point, and throwing away the good records before it would turn a partial
// loss into a total one. For a framed stream it matters less, because the reader resyncs past
// damage and keeps going -- which is the whole reason the framed container exists.
func ReadBatch(b Batch) ([]Record, error) {
	if b.Container == ContainerFrames {
		return readFramedBatch(b)
	}
	return readTarGzBatch(b)
}

// readFramedBatch decodes a framed zstd stream. Damage inside it costs one frame: the reader
// scans forward to the next frame magic and carries on, so the error returned here describes what
// was skipped rather than what stopped the read.
func readFramedBatch(b Batch) ([]Record, error) {
	raw, err := os.ReadFile(b.Path)
	if err != nil {
		return nil, err
	}
	var out []Record
	res, err := dropboxframe.Read(raw, func(rec []byte) error {
		ad, perr := classad.ParseOld(string(rec))
		if perr != nil {
			// One unparseable record does not cost the frame, let alone the batch.
			return nil
		}
		out = append(out, Record{Batch: b, Entry: fmt.Sprintf("frame record %d", len(out)), Ad: ad})
		return nil
	})
	if err != nil {
		return out, fmt.Errorf("%s: %w", filepath.Base(b.Path), err)
	}
	if len(res.Problems) > 0 {
		return out, fmt.Errorf("%s: recovered %d records across %d frames; %d frame(s) damaged: %s",
			filepath.Base(b.Path), res.Records, res.Frames, res.Damaged, strings.Join(res.Problems, "; "))
	}
	return out, nil
}

// readTarGzBatch decodes the original gzip tarball container.
func readTarGzBatch(b Batch) ([]Record, error) {
	f, err := os.Open(b.Path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", filepath.Base(b.Path), err)
	}
	defer zr.Close()

	var out []Record
	tr := tar.NewReader(zr)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return out, fmt.Errorf("%s: after %d records: %w", filepath.Base(b.Path), len(out), err)
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			return out, fmt.Errorf("%s: reading %s: %w", filepath.Base(b.Path), hdr.Name, err)
		}
		ad, err := classad.ParseOld(string(data))
		if err != nil {
			// One unparseable record is not a reason to drop the batch: the point of a row-wise
			// format is that records fail independently.
			continue
		}
		out = append(out, Record{Batch: b, Entry: hdr.Name, Ad: ad})
	}
}
