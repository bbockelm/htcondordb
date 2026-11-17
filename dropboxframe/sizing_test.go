package dropboxframe

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"testing"
)

// TestFrameSizeTradeoff prints the compression/recoverability tradeoff across frame sizes against
// the current format (one gzip tarball per 2000-record batch), so the default is a measured choice
// rather than a guess. It asserts only the shape of the curve; the numbers are for the design note.
func TestFrameSizeTradeoff(t *testing.T) {
	const n = 2000 // the exporter's default RollJobs: one batch

	var raw int
	recs := make([][]byte, n)
	for i := range recs {
		recs[i] = variedJobRecord(i)
		raw += len(recs[i])
	}

	// Baseline: the format today, one gzip stream over a tar of one file per record.
	var tgz bytes.Buffer
	zw := gzip.NewWriter(&tgz)
	tw := tar.NewWriter(zw)
	for i, r := range recs {
		if err := tw.WriteHeader(&tar.Header{Name: fmt.Sprintf("%06d.classad", i), Mode: 0o600, Size: int64(len(r))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(r); err != nil {
			t.Fatal(err)
		}
	}
	tw.Close()
	zw.Close()

	t.Logf("%d records, %d bytes raw", n, raw)
	t.Logf("%-22s %10s %7s %12s %s", "format", "bytes", "ratio", "frames", "records per frame (blast radius)")
	t.Logf("%-22s %10d %6.2fx %12s %s", "tar.gz (today)", tgz.Len(), float64(raw)/float64(tgz.Len()), "1", fmt.Sprint(n))

	type row struct {
		size   int
		bytes  int
		frames int
	}
	var rows []row
	for _, size := range []int{MinFrameBytesFloor, 64 << 10, 256 << 10, 1 << 20, 4 << 20} {
		var buf bytes.Buffer
		w, err := NewWriter(&buf, size)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range recs {
			if err := w.Write(r); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		frames, _, _ := w.Stats()
		rows = append(rows, row{size, buf.Len(), frames})
		perFrame := n
		if frames > 0 {
			perFrame = n / frames
		}
		t.Logf("%-22s %10d %6.2fx %12d %d", fmt.Sprintf("framed, %dKiB", size>>10), buf.Len(), float64(raw)/float64(buf.Len()), frames, perFrame)

		got, res := readAll(t, buf.Bytes())
		if len(got) != n || res.Damaged != 0 {
			t.Errorf("framed %dKiB did not round-trip: %d records, %d damaged", size>>10, len(got), res.Damaged)
		}
	}

	// Frames at the floor are expected to be bad -- that is why there is a floor. What must hold
	// is that a sensible frame size is not a compression regression against the format today.
	for _, r := range rows {
		if r.size < 64<<10 {
			continue
		}
		if r.bytes > tgz.Len() {
			t.Errorf("frame size %dKiB produced %d bytes, worse than the %d a single tar.gz costs",
				r.size>>10, r.bytes, tgz.Len())
		}
	}
	// And the floor must be visibly worse than a sensible size, or the floor is pointless.
	if rows[0].bytes <= rows[2].bytes {
		t.Errorf("frames at the %dKiB floor cost %d bytes, no worse than %dKiB at %d -- the floor is not doing anything",
			rows[0].size>>10, rows[0].bytes, rows[2].size>>10, rows[2].bytes)
	}
}

// variedJobRecord builds a record with the variation real job ads have -- different owners,
// commands, working directories, resource requests and times -- so the compression numbers above
// are not an artifact of 2000 identical ads.
func variedJobRecord(i int) []byte {
	owners := []string{"alice", "bob", "carol", "dave", "erin", "frank", "grace", "heidi"}
	projects := []string{"CMS", "ATLAS", "IceCube", "LIGO", "DUNE"}
	cmds := []string{"run.sh", "analyze.py", "sim_wrapper.sh", "stage_and_go.sh", "fit.exe"}
	owner := owners[i%len(owners)]
	return []byte(fmt.Sprintf(
		"GlobalJobId = \"ap%d.chtc.wisc.edu#%d.%d#%d\"\nOwner = \"%s\"\n"+
			"ProjectName = \"%s\"\nClusterId = %d\nProcId = %d\nJobStatus = %d\n"+
			"RequestCpus = %d\nRequestMemory = %d\nRequestDisk = %d\n"+
			"Cmd = \"/home/%s/%s\"\nIwd = \"/home/%s/work/run%d\"\n"+
			"QDate = %d\nCompletionDate = %d\nRemoteWallClockTime = %d.0\n"+
			"NumJobStarts = %d\nExitCode = %d\nLastRemoteHost = \"slot%d@e%04d.chtc.wisc.edu\"\n",
		2000+i%40, 100000+i, i%10, 1763000000+i*17, owner,
		projects[i%len(projects)], 100000+i, i%10, 3+i%3,
		1+i%16, 1024*(1+i%8), 1024*1024*(1+i%20),
		owner, cmds[i%len(cmds)], owner, i%97,
		1763000000+i*17, 1763003600+i*23, 600+i*7%86400,
		1+i%4, i%3, i%64, i%900))
}
