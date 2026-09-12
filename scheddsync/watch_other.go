//go:build !linux

package scheddsync

import "log/slog"

// newPlatformWatcher has no implementation off Linux, so the tailers poll exactly as they
// did before. Only inotify is wired up: the daemon runs beside a condor_schedd, and the
// portability a kqueue/ReadDirectoryChangesW path would buy is latency on platforms that
// do not deploy this.
func newPlatformWatcher(string, *slog.Logger) fileWatcher { return nopWatcher{} }
