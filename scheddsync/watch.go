package scheddsync

import "log/slog"

// fileWatcher reports that a tailed file may have changed, so the tailer can poll early
// instead of waiting out its timer. It is STRICTLY an optimization: every guarantee the
// mirror makes comes from polling, and a watcher that never fires only costs latency.
//
// That framing is not defensive styling, it is forced by what inotify actually does. Three
// ways it goes quiet, two of them verified against a real kernel:
//
//   - A rotation destroys a file watch. The schedd compacts by writing job_queue.log.tmp
//     and renaming it over job_queue.log; the kernel then delivers IN_IGNORED and never
//     speaks about that path again. A watch on the file alone is deaf from the first
//     compaction onward -- which is why the directory is watched too, and why IN_IGNORED
//     re-arms rather than being ignored.
//   - The event queue overflows silently. It holds 16384 events; past that the kernel
//     drops them and sets IN_Q_OVERFLOW once. Flooding a watch with ~120k events dropped
//     103,615 of them behind that single flag.
//   - Network filesystems deliver nothing for a writer on another host, since the hooks
//     are in the local VFS. A mirror reading a shared SPOOL from a different machine would
//     be permanently deaf with no error to report.
//
// Any of those degrades to "the poll notices it on the next tick", which is exactly the
// behavior with no watcher at all.
type fileWatcher interface {
	// Changed is signalled, coalesced, when the file may have changed. A nil channel is a
	// valid return and is what unsupported platforms give: a receive on nil blocks forever,
	// so the select arm is inert without the caller testing for it.
	Changed() <-chan struct{}
	Close()
}

// nopWatcher is the watcher for platforms with no implementation, and the fallback when
// one cannot be established.
type nopWatcher struct{}

func (nopWatcher) Changed() <-chan struct{} { return nil }
func (nopWatcher) Close()                   {}

// newWatcher returns a watcher for path, or a no-op one if watching is unavailable or
// disabled. It never returns nil and never returns an error: failing to watch is not a
// failure to mirror.
func newWatcher(path string, disabled bool, log *slog.Logger) fileWatcher {
	if disabled || path == "" {
		return nopWatcher{}
	}
	return newPlatformWatcher(path, log)
}
