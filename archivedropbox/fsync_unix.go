package archivedropbox

import (
	"errors"
	"syscall"
)

// isFsyncDirUnsupported reports whether an error from fsync(2) on a directory means the platform/
// filesystem simply does not support it (rather than a real I/O failure). Directory fsync is
// unsupported or a no-op on several filesystems, which surface it as EINVAL/ENOTSUP/EBADF.
func isFsyncDirUnsupported(err error) bool {
	return errors.Is(err, syscall.EINVAL) ||
		errors.Is(err, syscall.ENOTSUP) ||
		errors.Is(err, syscall.EBADF)
}
