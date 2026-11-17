package archivedropbox

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/bbockelm/htcondordb/dropboxframe"
)

// writeFramed writes a batch as a framed zstd stream: batch-<seq>-<firstModUnix>.cadf.
//
// The records are the same bytes a tarball would hold -- the container changes, the serialization
// does not. What changes is what damage costs: a tarball is one gzip stream, so a bad byte loses
// everything after it, while a framed stream loses one frame and resyncs.
//
// The per-record name a tarball carries in its entry header is not written here. It exists in the
// tarball because tar needs a name; the record itself carries its GlobalJobId, which is what the
// restore path dedups on, so the framed stream stores the record and nothing else.
func (w *dropboxWriter) writeFramed(seq uint64, recs []record) (string, error) {
	final := filepath.Join(w.dir, fmt.Sprintf("batch-%08d-%d%s", seq, recs[0].modUnix, ContainerFrames.batchExt()))
	f, err := os.CreateTemp(w.dir, tmpPrefix+"batch-*.tmp")
	if err != nil {
		return "", fmt.Errorf("archivedropbox: temp file: %w", err)
	}
	tmp := f.Name()
	cleanup := func() { f.Close(); os.Remove(tmp) }

	fw, err := dropboxframe.NewWriter(f, w.minFrame)
	if err != nil {
		cleanup()
		return "", err
	}
	for _, r := range recs {
		if err := fw.Write([]byte(r.body)); err != nil {
			cleanup()
			return "", err
		}
	}
	if err := fw.Close(); err != nil {
		cleanup()
		return "", err
	}
	// Same durability order as the tarball path: data to disk, then the rename, then the
	// directory, and only then may the caller advance its cursor.
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
