package archivedropbox

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// ByteSize is a byte count that (de)serializes as a human size with a unit prefix. It accepts a
// bare number (bytes) or a value with an IEC prefix (KiB/MiB/GiB/TiB and the shorthand K/M/G/T,
// all 1024-based -- matching HTCondor's convention for sizes) or an SI prefix (KB/MB/GB/TB,
// 1000-based). It marshals back to the byte count so the stored config is unambiguous.
type ByteSize int64

func (b ByteSize) MarshalJSON() ([]byte, error) { return json.Marshal(int64(b)) }

func (b *ByteSize) UnmarshalJSON(data []byte) error {
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	switch x := v.(type) {
	case float64:
		*b = ByteSize(int64(x))
		return nil
	case string:
		n, err := ParseByteSize(x)
		if err != nil {
			return err
		}
		*b = n
		return nil
	default:
		return fmt.Errorf("invalid byte size %v", v)
	}
}

// unit multipliers, longest suffix first so "GiB" is matched before "G"/"B".
var byteUnits = []struct {
	suffix string
	mult   int64
}{
	{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}, {"TiB", 1 << 40},
	{"KB", 1e3}, {"MB", 1e6}, {"GB", 1e9}, {"TB", 1e12},
	{"K", 1 << 10}, {"M", 1 << 20}, {"G", 1 << 30}, {"T", 1 << 40},
	{"B", 1},
}

// ParseByteSize parses a size like "2GiB", "512MB", "1048576", or "2G" into a byte count.
func ParseByteSize(s string) (ByteSize, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("archivedropbox: empty byte size")
	}
	// Longest matching suffix wins (so "GiB" is not read as "G" + "iB").
	best := ""
	var mult int64 = 1
	for _, u := range byteUnits {
		if strings.HasSuffix(s, u.suffix) && len(u.suffix) > len(best) {
			best, mult = u.suffix, u.mult
		}
	}
	num := strings.TrimSpace(strings.TrimSuffix(s, best))
	if num == "" {
		return 0, fmt.Errorf("archivedropbox: byte size %q has no number", s)
	}
	// Allow a fractional value ("1.5GiB").
	f, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return 0, fmt.Errorf("archivedropbox: invalid byte size %q: %w", s, err)
	}
	if f < 0 {
		return 0, fmt.Errorf("archivedropbox: negative byte size %q", s)
	}
	return ByteSize(int64(f * float64(mult))), nil
}

func (b ByteSize) String() string { return strconv.FormatInt(int64(b), 10) }
