package leaderfollower

import "errors"

// Watch event kinds, matching db.WatchKind / the dbrpc.WatchEvent.Kind wire
// encoding (0 upsert, 1 delete, 2 reset).
const (
	wkUpsert uint8 = 0
	wkDelete uint8 = 1
	wkReset  uint8 = 2
)

var errStreamEnded = errors.New("leaderfollower: leader closed the commit stream")

func errConfig(msg string) error { return errors.New("leaderfollower: " + msg) }
