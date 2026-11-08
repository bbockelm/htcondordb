package bench

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Guarding the scratch directory.
//
// Benchmark mode serves generated data with ANONYMOUS READS and an open Flight endpoint. That is
// the right default for a throwaway server holding invented job ads, and badly wrong for anything
// else -- pointed at a spool it would publish a real pool's queue to whoever connects.
//
// Nothing about a directory path says which it is, and the mistake is one keystroke: a stale
// shell, a copied command line, HTCONDORDB_DIR already exported. So the mode refuses any directory
// it cannot prove it owns. It owns one that is empty, or one carrying the marker it writes when it
// takes ownership; anything else is refused by name, including a directory holding a database it
// did not create.
//
// The marker is written only after the directory is found safe, so a refusal never leaves a mark
// that would make the next attempt succeed.

// ScratchMarker is the file that marks a directory as benchmark scratch.
const ScratchMarker = ".htcondordb-benchmark-scratch"

// ErrNotScratch is returned when a directory cannot be proven to be benchmark scratch.
var ErrNotScratch = errors.New("bench: directory is not benchmark scratch")

// GuardScratchDir proves that dir is safe to use as benchmark scratch, creating and marking it if
// necessary. It returns ErrNotScratch (wrapped, with what it found) for anything it does not own.
//
// Call this BEFORE opening a store, enabling anonymous access or binding a Flight endpoint: it is
// the only thing standing between a mistyped path and an unauthenticated copy of real data.
func GuardScratchDir(dir string) error {
	if strings.TrimSpace(dir) == "" {
		return fmt.Errorf("%w: no directory given", ErrNotScratch)
	}
	info, err := os.Stat(dir)
	switch {
	case errors.Is(err, os.ErrNotExist):
		if mkErr := os.MkdirAll(dir, 0o750); mkErr != nil {
			return fmt.Errorf("creating scratch directory %s: %w", dir, mkErr)
		}
		return writeMarker(dir)
	case err != nil:
		return fmt.Errorf("inspecting %s: %w", dir, err)
	case !info.IsDir():
		return fmt.Errorf("%w: %s is not a directory", ErrNotScratch, dir)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("reading %s: %w", dir, err)
	}
	var names []string
	for _, e := range entries {
		if e.Name() == ScratchMarker {
			return nil // ours already
		}
		names = append(names, e.Name())
	}
	if len(names) == 0 {
		return writeMarker(dir)
	}
	// Non-empty and unmarked. Name what is there, because the operator's next question is
	// "which directory did I just point it at".
	shown := names
	if len(shown) > 5 {
		shown = append(append([]string{}, names[:5]...), fmt.Sprintf("... and %d more", len(names)-5))
	}
	return fmt.Errorf("%w: %s is not empty and carries no %s marker (found: %s). "+
		"Benchmark mode serves generated data with anonymous reads, so it will not open a directory "+
		"it did not create. Point it at an empty directory, or remove this one if it is scratch",
		ErrNotScratch, dir, ScratchMarker, strings.Join(shown, ", "))
}

func writeMarker(dir string) error {
	body := fmt.Sprintf("htcondordb benchmark scratch\ncreated: %s\n\n"+
		"This directory is served with ANONYMOUS READS while benchmark mode runs.\n"+
		"Delete this file to stop benchmark mode from reusing the directory.\n",
		time.Now().UTC().Format(time.RFC3339))
	if err := os.WriteFile(filepath.Join(dir, ScratchMarker), []byte(body), 0o640); err != nil {
		return fmt.Errorf("marking %s as scratch: %w", dir, err)
	}
	return nil
}
