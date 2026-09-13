//go:build linux

package scheddsync

import (
	"bytes"
	"log/slog"
	"path/filepath"
	"sync"
	"unsafe"

	"golang.org/x/sys/unix"
)

// inotifyWatcher watches a tailed file two ways at once, because one is not enough.
//
// The file watch reports appends (IN_MODIFY), which is the common case and all a naive
// implementation would use. But the schedd replaces the log by renaming a temp file over
// it, and the kernel responds by dropping the watch: IN_IGNORED arrives and nothing is
// ever reported for that path again, because the watch followed the now-orphaned inode.
// The directory watch is what survives -- it reports IN_MOVED_TO for the basename -- and
// it is what triggers re-arming the file watch on the replacement.
//
// The directory is the schedd's SPOOL, which is busy with files this tailer does not care
// about, so directory events are filtered to the exact basename. A stray wake would only
// cost one cheap poll, but there is no reason to take them.
type inotifyWatcher struct {
	fd    int
	wakeR int // self-pipe: the only way to interrupt a blocking Poll with no idle wakeups
	wakeW int
	ch    chan struct{}

	dir  string
	base string
	path string
	log  *slog.Logger

	mu     sync.Mutex
	fileWD int // -1 when not currently armed (the file may not exist)
	dirWD  int

	closeOnce sync.Once
	done      chan struct{}
}

func newPlatformWatcher(path string, log *slog.Logger) fileWatcher {
	fd, err := unix.InotifyInit1(unix.IN_CLOEXEC | unix.IN_NONBLOCK)
	if err != nil {
		// EMFILE here means fs.inotify.max_user_instances is exhausted, which is a host
		// condition and not this daemon's to fix. Polling still mirrors correctly.
		log.Info("scheddsync: inotify unavailable; falling back to polling only",
			"file", path, "err", err.Error())
		return nopWatcher{}
	}
	var pipe [2]int
	if err := unix.Pipe2(pipe[:], unix.O_CLOEXEC|unix.O_NONBLOCK); err != nil {
		_ = unix.Close(fd)
		log.Info("scheddsync: inotify shutdown pipe unavailable; polling only", "err", err.Error())
		return nopWatcher{}
	}
	w := &inotifyWatcher{
		fd: fd, wakeR: pipe[0], wakeW: pipe[1],
		ch:   make(chan struct{}, 1),
		dir:  filepath.Dir(path),
		base: filepath.Base(path),
		path: path, log: log,
		fileWD: -1, dirWD: -1,
		done: make(chan struct{}),
	}
	// The directory watch is the one that must succeed: it is what notices the file being
	// created or replaced. Without it there is nothing to re-arm from, so degrade to polling.
	dirWD, err := unix.InotifyAddWatch(w.fd, w.dir, unix.IN_CREATE|unix.IN_MOVED_TO)
	if err != nil {
		w.closeFDs()
		log.Info("scheddsync: cannot watch directory; polling only", "dir", w.dir, "err", err.Error())
		return nopWatcher{}
	}
	w.dirWD = dirWD
	w.armFile() // may fail: a history file often does not exist yet, and CREATE will arm it
	go w.run()
	return w
}

func (w *inotifyWatcher) Changed() <-chan struct{} { return w.ch }

// armFile (re)establishes the watch on the file itself. Failure is normal and not an
// error: the file may not exist yet, or may have just been renamed away. The directory
// watch stays armed either way and will report the replacement.
func (w *inotifyWatcher) armFile() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.fileWD >= 0 {
		// Best-effort: the kernel may have dropped it already (IN_IGNORED), in which case
		// this is a no-op error we do not care about.
		_, _ = unix.InotifyRmWatch(w.fd, uint32(w.fileWD))
		w.fileWD = -1
	}
	wd, err := unix.InotifyAddWatch(w.fd, w.path,
		unix.IN_MODIFY|unix.IN_ATTRIB|unix.IN_MOVE_SELF|unix.IN_DELETE_SELF)
	if err == nil {
		w.fileWD = wd
	}
}

func (w *inotifyWatcher) wds() (fileWD, dirWD int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.fileWD, w.dirWD
}

// signal announces a change without ever blocking the reader goroutine. The channel holds
// one token: a burst of events collapses into a single wake, which is what the tailer
// wants -- it re-reads to EOF regardless of how many writes it was told about.
func (w *inotifyWatcher) signal() {
	select {
	case w.ch <- struct{}{}:
	default:
	}
}

func (w *inotifyWatcher) run() {
	defer close(w.done)
	buf := make([]byte, 16*1024) // reused; one read drains many events
	for {
		fds := []unix.PollFd{
			{Fd: int32(w.fd), Events: unix.POLLIN},
			{Fd: int32(w.wakeR), Events: unix.POLLIN},
		}
		// Block indefinitely. The self-pipe, not a timeout, is what wakes this for
		// shutdown -- a timeout would put back the periodic idle wakeups this whole
		// change exists to avoid.
		if _, err := unix.Poll(fds, -1); err != nil {
			if err == unix.EINTR {
				continue
			}
			return
		}
		if fds[1].Revents != 0 {
			return // Close was called
		}
		n, err := unix.Read(w.fd, buf)
		if err != nil {
			if err == unix.EAGAIN || err == unix.EINTR {
				continue
			}
			return
		}
		if n > 0 {
			w.handle(buf[:n])
		}
	}
}

// handle walks one read's worth of events and decides two things: whether to wake the
// tailer, and whether the file watch has to be re-established.
func (w *inotifyWatcher) handle(b []byte) {
	fileWD, dirWD := w.wds()
	var wake, rearm bool
	for off := 0; off+unix.SizeofInotifyEvent <= len(b); {
		ev := (*unix.InotifyEvent)(unsafe.Pointer(&b[off]))
		nameLen := int(ev.Len)
		end := off + unix.SizeofInotifyEvent + nameLen
		if end > len(b) {
			break // truncated tail; the next read continues
		}
		name := ""
		if nameLen > 0 {
			raw := b[off+unix.SizeofInotifyEvent : end]
			if i := bytes.IndexByte(raw, 0); i >= 0 {
				raw = raw[:i]
			}
			name = string(raw)
		}
		off = end

		switch {
		case ev.Mask&unix.IN_Q_OVERFLOW != 0:
			// An unknown number of events was dropped, so nothing about the current state
			// can be assumed -- including whether the file was replaced while we were not
			// looking. Wake and re-arm; the poll itself does full rotation detection, so
			// this recovers rather than merely resyncing the watch.
			wake, rearm = true, true
		case int(ev.Wd) == dirWD:
			if name == w.base {
				wake, rearm = true, true // created or renamed into place
			}
		case int(ev.Wd) == fileWD:
			wake = true
			if ev.Mask&(unix.IN_IGNORED|unix.IN_MOVE_SELF|unix.IN_DELETE_SELF) != 0 {
				rearm = true
			}
		}
	}
	if rearm {
		w.armFile()
	}
	if wake {
		w.signal()
	}
}

func (w *inotifyWatcher) closeFDs() {
	_ = unix.Close(w.fd)
	_ = unix.Close(w.wakeR)
	_ = unix.Close(w.wakeW)
}

func (w *inotifyWatcher) Close() {
	w.closeOnce.Do(func() {
		// Wake the blocked Poll through the pipe and let run() return before closing the
		// descriptors, so it never reads from a fd that has been closed (and possibly
		// reused) underneath it.
		_, _ = unix.Write(w.wakeW, []byte{0})
		<-w.done
		w.closeFDs()
	})
}
