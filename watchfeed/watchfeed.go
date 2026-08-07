// Package watchfeed streams a database change feed as already-decoded ClassAds, over
// whichever transport the server supports.
//
// The server can send each event's ad two ways: as ClassAd text (dbrpc.WatchTable, which
// every server speaks) or in wire form (dbrpc.WatchWireTable, classad v0.25.1 and later).
// The wire form is what the ad already is in storage, so taking it costs the server a
// little less and costs this side about a sixth -- a decode instead of a parse, ~1.8us
// against ~9.0us on a job ad. A change feed is unbounded and has many consumers, so that
// difference is paid on every event by each of them, forever.
//
// Every consumer wants the same thing (a ClassAd) and has the same fallback to write, so it
// lives here once: Watch prefers the wire feed and falls back to the text feed against a
// server that does not implement it.
package watchfeed

import (
	"context"
	"errors"
	"fmt"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// Event is one change, with its ad decoded. Ad is nil for a delete and for the control
// events (reset, synced, resync), which carry no ad -- exactly as AdText is empty for those
// on the text feed.
//
// Err is set when the event's ad could not be decoded. The event is still delivered, with a
// nil Ad, so a consumer can log and continue rather than have one malformed ad silently
// disappear from the stream (or kill it).
type Event struct {
	Kind   uint8 // see db.WatchKind: 0 upsert, 1 delete, 2 reset, 3 synced, 4 resync
	Key    string
	Ad     *classad.ClassAd
	Cursor []byte
	Err    error
}

// Watch streams decoded change events for table, resuming after cursor (nil = from now).
// The returned stop function ends the stream, as with the dbrpc watch calls it wraps.
//
// It uses the wire-form feed where the server has it and the text feed otherwise, and
// reports which in the returned bool, so a caller can say so in a log line.
func Watch(ctx context.Context, c *dbrpc.Client, table string, cursor []byte) (<-chan Event, func(), bool, error) {
	wireCh, stop, err := c.WatchWireTable(ctx, table, cursor)
	if err == nil {
		return fromWire(ctx, wireCh), stop, true, nil
	}
	if !errors.Is(err, dbrpc.ErrWatchWireUnsupported) {
		return nil, nil, false, err
	}
	// Server predates the wire feed: take the text feed, which every server speaks.
	textCh, stop, err := c.WatchTable(ctx, table, cursor)
	if err != nil {
		return nil, nil, false, err
	}
	return fromText(ctx, textCh), stop, false, nil
}

// fromWire adapts the wire feed, decoding each ad.
func fromWire(ctx context.Context, in <-chan dbrpc.WireWatchEvent) <-chan Event {
	out := make(chan Event, 64)
	go func() {
		defer close(out)
		for ev := range in {
			e := Event{Kind: ev.Kind, Key: ev.Key, Cursor: ev.Cursor}
			ad, err := dbrpc.DecodeWatchAd(ev.Ad)
			if err != nil {
				e.Err = fmt.Errorf("decoding the wire ad for %q: %w", ev.Key, err)
			} else {
				e.Ad = ad
			}
			select {
			case out <- e:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// fromText adapts the text feed, parsing each ad. The server streams the new-ClassAd
// (bracketed) form here, unlike the projected query ops, so this parses with Parse.
func fromText(ctx context.Context, in <-chan dbrpc.WatchEvent) <-chan Event {
	out := make(chan Event, 64)
	go func() {
		defer close(out)
		for ev := range in {
			e := Event{Kind: ev.Kind, Key: ev.Key, Cursor: ev.Cursor}
			if ev.AdText != "" {
				ad, err := classad.Parse(ev.AdText)
				if err != nil {
					e.Err = fmt.Errorf("parsing the ad for %q: %w", ev.Key, err)
				} else {
					e.Ad = ad
				}
			}
			select {
			case out <- e:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}
