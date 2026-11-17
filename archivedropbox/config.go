// Package archivedropbox exports an htcondordb archive's job records as compressed tarballs
// dropped into a directory for an out-of-band consumer to ingest. Like kafkasync/opensearchsync
// it is a standalone module + binary (a dbrpc client), so it never enters the core daemon build;
// it registers via the generic exporter registry with Kind = "dropbox".
//
// It targets an append-only ARCHIVE (e.g. the history archive). Records are batched and written
// as a .tar.gz once every RollJobs records OR every RollInterval, whichever comes first, one
// file per job -- old-ClassAd text by default, or JSON (condor_history -json's rendering) when
// the exporter is configured with format "json". Each tarball is written to a temp file, fsync'd, atomically renamed into
// place, and the containing directory fsync'd; only then does the resume cursor advance -- so a
// crash never advances past data that is not durably on disk (at-least-once).
//
// Backpressure: a consumer drains the directory. If the directory already holds MaxDropboxBytes
// or more of data, the exporter stops writing (and stops advancing the cursor) until the consumer
// catches up -- it never floods a full dropbox.
//
// Data loss: the archive has finite retention. If the exporter falls far enough behind that its
// resume cursor is pruned, the watch re-syncs from the oldest retained record; the exporter drops
// a small ClassAd into the dropbox describing the estimated lost time range, then resumes exporting
// from the oldest available record.
package archivedropbox

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/bbockelm/htcondordb/dropboxframe"
)

// Kind is the exporter registry kind for this exporter.
const Kind = "dropbox"

// Config is one dropbox exporter's definition, stored opaquely in ExporterDef.Config.
type Config struct {
	// Table is the source archive whose records are exported (e.g. "history").
	Table string `json:"table"`

	// Directory is the dropbox: where finished tarballs (and any loss-report ClassAds) are
	// placed for the consumer. Required. The exporter writes temp files here too (dot-prefixed),
	// so it must be on a single filesystem and writable by the exporter's user.
	Directory string `json:"directory"`

	// RollJobs rolls a tarball once this many records have accumulated. Default 2000.
	RollJobs int `json:"rollJobs,omitempty"`
	// RollInterval rolls a (partial) tarball once this long has passed with records buffered,
	// bounding how long a record waits before it is durable. Default 10m.
	RollInterval Duration `json:"rollInterval,omitempty"`

	// MaxDropboxBytes is the backpressure ceiling: while the dropbox already holds at least this
	// many bytes of undelivered data, the exporter writes nothing and does not advance its cursor.
	// Accepts a byte count or a size with a unit prefix ("2GiB", "2GB", "512MB"). Default 2GiB.
	MaxDropboxBytes ByteSize `json:"maxDropboxBytes,omitempty"`

	// CompressionLevel is the gzip level (gzip.BestSpeed=1 .. gzip.BestCompression=9). Default 6
	// (gzip.DefaultCompression).
	CompressionLevel int `json:"compressionLevel,omitempty"`

	// Format is how each record is serialized inside the tarball: FormatClassAd (the default,
	// old-ClassAd text, one .classad entry per record) or FormatJSON (one .json entry per record).
	Format RecordFormat `json:"format,omitempty"`

	// Container is how a batch is packaged on disk: ContainerTarGz (the default, one
	// gzip-compressed tar per batch) or ContainerFrames (a framed zstd stream).
	//
	// It is orthogonal to Format, which chooses how one record is serialized. A framed stream
	// holds exactly the same record bytes; it changes only how they are packaged.
	//
	// The default stays ContainerTarGz because the dropbox exists for an out-of-band consumer,
	// and those consumers are external code reading gzip tarballs with standard tools. Switch a
	// dropbox to ContainerFrames only once its consumer understands the framed format.
	Container ContainerFormat `json:"container,omitempty"`

	// MinFrameBytes is how many bytes of records a frame accumulates before it is closed, with
	// ContainerFrames. 0 uses the default; anything below the floor is raised to it.
	//
	// This is the knob that decides what damage costs: a bad frame loses the records in that
	// frame and nothing else. It must be meaningfully smaller than a batch to bound anything --
	// a frame size at or above RollJobs worth of records yields one frame per batch, which is
	// the same blast radius as a tarball.
	MinFrameBytes int `json:"minFrameBytes,omitempty"`
}

// ContainerFormat selects how a batch is packaged on disk.
type ContainerFormat string

const (
	// ContainerTarGz writes each batch as one gzip-compressed tar: batch-<seq>-<mod>.tar.gz.
	// Readable with standard tools, and the default. Its weakness is that a tarball is a single
	// gzip stream, so damage costs everything after it -- up to a whole RollJobs batch.
	ContainerTarGz ContainerFormat = "targz"

	// ContainerFrames writes each batch as a framed zstd stream: batch-<seq>-<mod>.cadf. Each
	// frame is independently decompressible and covered by its own checksum, and a reader that
	// hits damage resyncs to the next frame, so a bad byte costs one frame rather than the batch.
	ContainerFrames ContainerFormat = "frames"
)

// batchExt is the file extension a container writes.
func (c ContainerFormat) batchExt() string {
	if c == ContainerFrames {
		return ".cadf"
	}
	return ".tar.gz"
}

// RecordFormat selects the on-disk serialization of a record inside the tarball.
type RecordFormat string

const (
	// FormatClassAd writes each record as old-ClassAd text ("Attr = value" lines), the format
	// condor_history prints and classad.Parse reads. Entries are named "<n>-<id>.classad".
	FormatClassAd RecordFormat = "classad"
	// FormatJSON writes each record as a JSON object, matching `condor_history -json`: ClassAd
	// expressions that are not literals are rendered as the string "/Expr(<expr>)/". Entries are
	// named "<n>-<id>.json".
	FormatJSON RecordFormat = "json"
)

// entryExt is the tar entry extension for a format.
func (f RecordFormat) entryExt() string {
	if f == FormatJSON {
		return ".json"
	}
	return ".classad"
}

// lossExt is the extension for a loss report dropped beside the tarballs. The ClassAd form keeps
// the historical ".ad" so an existing consumer's glob still matches.
func (f RecordFormat) lossExt() string {
	if f == FormatJSON {
		return ".json"
	}
	return ".ad"
}

// Defaults, exported so the cmd help and tests can reference them.
const (
	DefaultRollJobs         = 2000
	DefaultRollInterval     = 10 * time.Minute
	DefaultMaxDropboxBytes  = ByteSize(2 * 1024 * 1024 * 1024) // 2 GiB
	DefaultCompressionLevel = 6                                // gzip.DefaultCompression
	DefaultFormat           = FormatClassAd
	DefaultContainer        = ContainerTarGz
)

// Validate fills defaults and checks required fields.
func (c *Config) Validate() error {
	if c.Table == "" {
		return fmt.Errorf("archivedropbox: table is required")
	}
	if c.Directory == "" {
		return fmt.Errorf("archivedropbox: directory is required")
	}
	if c.RollJobs <= 0 {
		c.RollJobs = DefaultRollJobs
	}
	if c.RollInterval <= 0 {
		c.RollInterval = Duration(DefaultRollInterval)
	}
	if c.MaxDropboxBytes <= 0 {
		c.MaxDropboxBytes = DefaultMaxDropboxBytes
	}
	if c.CompressionLevel == 0 {
		c.CompressionLevel = DefaultCompressionLevel
	}
	// gzip accepts DefaultCompression(-1), NoCompression(0), and 1..9; 0 was mapped to the
	// default above, so only guard the out-of-range case.
	if c.CompressionLevel < -1 || c.CompressionLevel > 9 {
		return fmt.Errorf("archivedropbox: compressionLevel %d out of range (-1..9)", c.CompressionLevel)
	}
	if c.Format == "" {
		c.Format = DefaultFormat
	}
	switch c.Format {
	case FormatClassAd, FormatJSON:
	default:
		return fmt.Errorf("archivedropbox: format %q is invalid (want %q or %q)", c.Format, FormatClassAd, FormatJSON)
	}
	if c.Container == "" {
		c.Container = DefaultContainer
	}
	switch c.Container {
	case ContainerTarGz:
		if c.MinFrameBytes != 0 {
			return fmt.Errorf("archivedropbox: minFrameBytes applies only to container %q", ContainerFrames)
		}
	case ContainerFrames:
		if c.MinFrameBytes == 0 {
			c.MinFrameBytes = dropboxframe.DefaultMinFrameBytes
		}
		if c.MinFrameBytes < dropboxframe.MinFrameBytesFloor {
			return fmt.Errorf("archivedropbox: minFrameBytes %d is below the %d floor, where a framed stream compresses worse than a tarball",
				c.MinFrameBytes, dropboxframe.MinFrameBytesFloor)
		}
	default:
		return fmt.Errorf("archivedropbox: container %q is invalid (want %q or %q)", c.Container, ContainerTarGz, ContainerFrames)
	}
	return nil
}

// Marshal serializes the config for storage in an ExporterDef.
func (c Config) Marshal() ([]byte, error) { return json.Marshal(c) }

// ParseConfig unmarshals an exporter Config from its stored JSON and validates it.
func ParseConfig(raw []byte) (Config, error) {
	var c Config
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &c); err != nil {
			return c, fmt.Errorf("archivedropbox: parsing config: %w", err)
		}
	}
	if err := c.Validate(); err != nil {
		return c, err
	}
	return c, nil
}

// Duration is a JSON-friendly time.Duration ("10m", "30s"; a bare number is nanoseconds).
type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch x := v.(type) {
	case float64:
		*d = Duration(x)
	case string:
		dur, err := time.ParseDuration(x)
		if err != nil {
			return err
		}
		*d = Duration(dur)
	default:
		return fmt.Errorf("invalid duration %v", v)
	}
	return nil
}
