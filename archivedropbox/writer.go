package archivedropbox

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// tmpPrefix marks a file the exporter is still writing (or a loss report being staged). A consumer
// MUST ignore dot-prefixed files -- only fully-renamed tarballs (batch-*.tar.gz) and loss reports
// (loss-*.ad) are complete.
const tmpPrefix = "."

// record is one job queued for the next tarball.
type record struct {
	name    string // in-tarball entry name (unique within the batch, ordered)
	adText  string // the ClassAd, old (bracketless) text form
	modUnix int64  // record-time, used as the entry's mod time (0 -> now)
}

// dropboxWriter owns durable writes into the dropbox directory: it stages a temp file, fsyncs it,
// atomically renames it into place, and fsyncs the directory so the rename itself is durable
// before the caller advances its resume cursor.
type dropboxWriter struct {
	dir      string
	compress int // gzip level
}

func newDropboxWriter(dir string, compressLevel int) (*dropboxWriter, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("archivedropbox: creating dropbox %q: %w", dir, err)
	}
	return &dropboxWriter{dir: dir, compress: compressLevel}, nil
}

// NewWriter builds the production dropbox Writer from a (validated) Config.
func NewWriter(cfg Config) (Writer, error) {
	return newDropboxWriter(cfg.Directory, cfg.CompressionLevel)
}

// WriteTarball writes recs as a gzip-compressed tar to a temp file, fsyncs it, atomically renames
// it to batch-<seq>-<firstModUnix>.tar.gz, and fsyncs the directory. It returns the final path.
// On any error the temp file is removed so a partial tarball never lingers in the dropbox.
func (w *dropboxWriter) WriteTarball(seq uint64, recs []record) (string, error) {
	if len(recs) == 0 {
		return "", nil
	}
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
		body := []byte(r.adText)
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

// WriteLossReport durably drops a small ClassAd describing an estimated data-loss window, using
// the same stage-fsync-rename-fsyncdir dance so a consumer never sees a partial report.
func (w *dropboxWriter) WriteLossReport(adText string, detectedUnix int64) (string, error) {
	final := filepath.Join(w.dir, fmt.Sprintf("loss-%d.ad", detectedUnix))
	f, err := os.CreateTemp(w.dir, tmpPrefix+"loss-*.tmp")
	if err != nil {
		return "", err
	}
	tmp := f.Name()
	if _, err := f.WriteString(adText); err != nil {
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
