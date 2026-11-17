package archivedropbox

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bbockelm/htcondordb/dropboxmanifest"
)

// tmpPrefix marks a file the exporter is still writing (or a loss report being staged). A consumer
// MUST ignore dot-prefixed files -- only fully-renamed tarballs (batch-*.tar.gz) and loss reports
// (loss-*.ad / loss-*.json) are complete.
const tmpPrefix = "."

// record is one job queued for the next tarball.
type record struct {
	name    string // in-tarball entry name (unique within the batch, ordered)
	body    string // the serialized record (old-ClassAd text or JSON, per Config.Format)
	modUnix int64  // record-time, used as the entry's mod time (0 -> now)
}

// dropboxWriter owns durable writes into the dropbox directory: it stages a temp file, fsyncs it,
// atomically renames it into place, and fsyncs the directory so the rename itself is durable
// before the caller advances its resume cursor.
type dropboxWriter struct {
	dir       string
	compress  int // gzip level, for ContainerTarGz
	container ContainerFormat
	minFrame  int // for ContainerFrames

	// exporter and table are recorded in each batch's manifest so a dropbox is self-describing:
	// an operator holding only the directory can see what wrote it and where it came from.
	exporter string
	table    string
	// prevSeq is the sequence number of the last batch written, carried into the next batch's
	// manifest. A consumer drains a dropbox from the front, so the lowest sequence present is
	// arbitrary and a low gap is normal; PrevSeq is what distinguishes an ordinary drain from a
	// batch that went missing between two that are still here.
	prevSeq uint64
}

func newDropboxWriter(dir string, compressLevel int) (*dropboxWriter, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("archivedropbox: creating dropbox %q: %w", dir, err)
	}
	return &dropboxWriter{dir: dir, compress: compressLevel, container: ContainerTarGz}, nil
}

// NewWriter builds the production dropbox Writer from a (validated) Config.
func NewWriter(cfg Config) (Writer, error) {
	w, err := newDropboxWriter(cfg.Directory, cfg.CompressionLevel)
	if err != nil {
		return nil, err
	}
	if cfg.Container != "" {
		w.container = cfg.Container
	}
	w.minFrame = cfg.MinFrameBytes
	w.table = cfg.Table
	return w, nil
}

// NewNamedWriter is NewWriter plus the exporter's name, which goes into each batch's manifest so
// a dropbox says what wrote it. Prefer it wherever the name is known.
func NewNamedWriter(cfg Config, name string) (Writer, error) {
	w, err := NewWriter(cfg)
	if err != nil {
		return nil, err
	}
	if dw, ok := w.(*dropboxWriter); ok {
		dw.exporter = name
	}
	return w, nil
}

// WriteBatch writes recs as one batch file in the configured container, to a temp file, fsyncs
// it, atomically renames it into place, and fsyncs the directory. It returns the final path. On
// any error the temp file is removed so a partial batch never lingers in the dropbox.
//
// The durability dance is the same for both containers and is what makes delivery at-least-once:
// the data reaches disk before the rename, and the rename reaches disk before the caller is told
// it may advance its cursor.
func (w *dropboxWriter) WriteBatch(seq uint64, recs []record) (string, error) {
	if len(recs) == 0 {
		return "", nil
	}
	var path string
	var err error
	if w.container == ContainerFrames {
		path, err = w.writeFramed(seq, recs)
	} else {
		path, err = w.writeTarGz(seq, recs)
	}
	if err != nil {
		return "", err
	}
	w.writeBatchManifest(path, seq, recs)
	w.prevSeq = seq
	return path, nil
}

// writeBatchManifest records what the batch holds, beside it.
//
// Best effort, and deliberately so: the batch is already durable and renamed into place by the
// time this runs, and the caller is about to advance its cursor on the strength of that. Losing
// the manifest must not lose or re-export the batch, so a failure here leaves the batch
// unverifiable -- exactly how a batch written before manifests existed is treated -- rather than
// turning a metadata problem into a data one.
func (w *dropboxWriter) writeBatchManifest(path string, seq uint64, recs []record) {
	sum, n, err := dropboxmanifest.HashFile(path)
	if err != nil {
		return
	}
	m := &dropboxmanifest.Manifest{
		Format:    dropboxmanifest.Format,
		File:      filepath.Base(path),
		Seq:       seq,
		PrevSeq:   w.prevSeq,
		Records:   len(recs),
		FirstTime: recs[0].modUnix,
		LastTime:  recs[len(recs)-1].modUnix,
		Bytes:     n,
		SHA256:    sum,
		Container: string(w.container),
		Exporter:  w.exporter,
		Table:     w.table,
	}
	_ = dropboxmanifest.Write(path, m, fsyncDir)
}

// writeTarGz is the original container: one gzip-compressed tar per batch.
func (w *dropboxWriter) writeTarGz(seq uint64, recs []record) (string, error) {
	final := filepath.Join(w.dir, fmt.Sprintf("batch-%08d-%d.tar.gz", seq, recs[0].modUnix))
	f, err := os.CreateTemp(w.dir, tmpPrefix+"batch-*.tmp")
	if err != nil {
		return "", fmt.Errorf("archivedropbox: temp file: %w", err)
	}
	tmp := f.Name()
	// Any failure past this point must not leave the temp file behind.
	cleanup := func() { f.Close(); os.Remove(tmp) }

	gz, err := gzip.NewWriterLevel(f, w.compress)
	if err != nil {
		cleanup()
		return "", err
	}
	tw := tar.NewWriter(gz)
	for _, r := range recs {
		body := []byte(r.body)
		mod := time.Unix(r.modUnix, 0)
		if r.modUnix <= 0 {
			mod = time.Unix(0, 0)
		}
		hdr := &tar.Header{
			Name:    r.name,
			Mode:    0o640,
			Size:    int64(len(body)),
			ModTime: mod,
			Format:  tar.FormatPAX, // preserves long names / full mod times
		}
		if err := tw.WriteHeader(hdr); err != nil {
			cleanup()
			return "", err
		}
		if _, err := tw.Write(body); err != nil {
			cleanup()
			return "", err
		}
	}
	if err := tw.Close(); err != nil {
		cleanup()
		return "", err
	}
	if err := gz.Close(); err != nil {
		cleanup()
		return "", err
	}
	// Durability: the file's data must reach disk BEFORE the rename, and the rename (a directory
	// entry) must reach disk before we tell the caller it is safe to advance the cursor.
	if err := f.Sync(); err != nil {
		cleanup()
		return "", err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return "", err
	}
	if err := os.Rename(tmp, final); err != nil {
		os.Remove(tmp)
		return "", err
	}
	if err := fsyncDir(w.dir); err != nil {
		return "", err
	}
	return final, nil
}

// WriteLossReport durably drops a small record describing an estimated data-loss window, using
// the same stage-fsync-rename-fsyncdir dance so a consumer never sees a partial report. ext is the
// exporter format's extension, so a JSON dropbox holds only JSON.
func (w *dropboxWriter) WriteLossReport(body string, detectedUnix int64, ext string) (string, error) {
	final := filepath.Join(w.dir, fmt.Sprintf("loss-%d%s", detectedUnix, ext))
	f, err := os.CreateTemp(w.dir, tmpPrefix+"loss-*.tmp")
	if err != nil {
		return "", err
	}
	tmp := f.Name()
	if _, err := f.WriteString(body); err != nil {
		f.Close()
		os.Remove(tmp)
		return "", err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return "", err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return "", err
	}
	if err := os.Rename(tmp, final); err != nil {
		os.Remove(tmp)
		return "", err
	}
	if err := fsyncDir(w.dir); err != nil {
		return "", err
	}
	return final, nil
}

// DirSize sums the bytes of completed dropbox files (tarballs + loss reports). Temp files (dot-
// prefixed, still being written) are excluded, so a large in-progress write does not itself
// trip backpressure. This is the "how full is the dropbox" gauge the consumer's drain rate races.
func (w *dropboxWriter) DirSize() (int64, error) {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, e := range entries {
		if e.IsDir() || strings.HasPrefix(e.Name(), tmpPrefix) {
			continue
		}
		// Backpressure measures undelivered DATA against the operator's ceiling. A batch's
		// manifest is metadata about data already counted, so charging it too would quietly
		// shrink the ceiling that was configured -- by a larger fraction the smaller the batches.
		if strings.HasSuffix(e.Name(), dropboxmanifest.Suffix) {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			continue // raced with a consumer's unlink; ignore
		}
		total += fi.Size()
	}
	return total, nil
}

// fsyncDir flushes a directory's metadata (the rename) to disk. A platform/filesystem that does
// not support directory fsync reports EINVAL/ENOTSUP/EBADF; that is not fatal (the rename is still
// ordered by the prior file sync), so those are tolerated.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	if err := d.Sync(); err != nil && !isFsyncDirUnsupported(err) {
		return fmt.Errorf("archivedropbox: fsync dropbox dir: %w", err)
	}
	return nil
}
