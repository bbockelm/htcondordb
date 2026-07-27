package archivedropbox

import (
	"encoding/json"
	"testing"
	"time"
)

func TestParseByteSize(t *testing.T) {
	cases := []struct {
		in   string
		want int64
		err  bool
	}{
		{"1048576", 1 << 20, false},
		{"2GiB", 2 << 30, false},
		{"2GB", 2_000_000_000, false},
		{"512MB", 512_000_000, false},
		{"2G", 2 << 30, false}, // shorthand is IEC (1024-based)
		{"1.5GiB", int64(1.5 * (1 << 30)), false},
		{"4KiB", 4 << 10, false},
		{"100B", 100, false},
		{"", 0, true},
		{"GiB", 0, true},
		{"-3MB", 0, true},
		{"twelve", 0, true},
	}
	for _, c := range cases {
		got, err := ParseByteSize(c.in)
		if c.err {
			if err == nil {
				t.Errorf("ParseByteSize(%q) = %d, want error", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseByteSize(%q) unexpected error: %v", c.in, err)
			continue
		}
		if int64(got) != c.want {
			t.Errorf("ParseByteSize(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestByteSizeJSONRoundTrip(t *testing.T) {
	// A string with a prefix decodes to bytes and re-encodes as a plain byte count.
	var c Config
	if err := json.Unmarshal([]byte(`{"table":"history","directory":"/d","maxDropboxBytes":"2GiB"}`), &c); err != nil {
		t.Fatal(err)
	}
	if int64(c.MaxDropboxBytes) != 2<<30 {
		t.Fatalf("MaxDropboxBytes = %d, want %d", c.MaxDropboxBytes, 2<<30)
	}
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	var c2 Config
	if err := json.Unmarshal(raw, &c2); err != nil {
		t.Fatal(err)
	}
	if c2.MaxDropboxBytes != c.MaxDropboxBytes {
		t.Fatalf("round-trip MaxDropboxBytes = %d, want %d", c2.MaxDropboxBytes, c.MaxDropboxBytes)
	}
}

func TestConfigValidateDefaults(t *testing.T) {
	c := Config{Table: "history", Directory: "/d"}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if c.RollJobs != DefaultRollJobs {
		t.Errorf("RollJobs = %d, want %d", c.RollJobs, DefaultRollJobs)
	}
	if time.Duration(c.RollInterval) != DefaultRollInterval {
		t.Errorf("RollInterval = %v, want %v", time.Duration(c.RollInterval), DefaultRollInterval)
	}
	if c.MaxDropboxBytes != DefaultMaxDropboxBytes {
		t.Errorf("MaxDropboxBytes = %d, want %d", c.MaxDropboxBytes, DefaultMaxDropboxBytes)
	}
	if c.CompressionLevel != DefaultCompressionLevel {
		t.Errorf("CompressionLevel = %d, want %d", c.CompressionLevel, DefaultCompressionLevel)
	}
}

func TestConfigValidateRequired(t *testing.T) {
	if err := (&Config{Directory: "/d"}).Validate(); err == nil {
		t.Error("missing table should error")
	}
	if err := (&Config{Table: "history"}).Validate(); err == nil {
		t.Error("missing directory should error")
	}
	if err := (&Config{Table: "history", Directory: "/d", CompressionLevel: 42}).Validate(); err == nil {
		t.Error("out-of-range compression level should error")
	}
}
