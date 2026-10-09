package cedarsync

import (
	"context"
	"errors"
	"testing"

	"github.com/PelicanPlatform/classad/db/replicate"
	"github.com/PelicanPlatform/classad/dbrpc"

	"github.com/bbockelm/htcondordb/watchfeed"
)

// undecodableSink records Apply and ApplyUndecodable calls.
type undecodableSink struct {
	flushSink
	undecodable []replicate.Change
}

func (u *undecodableSink) ApplyUndecodable(c replicate.Change, _ error) error {
	u.undecodable = append(u.undecodable, c)
	return nil
}

// TestRunnerDeliversUndecodable: an upsert whose ad could not be decoded still reaches a sink that
// asks for it -- with its key and cursor, without an ad, and past the client-side constraint (which
// cannot evaluate it). A sink reconciling a Reset replay needs it: dropped, the key would look
// absent at the source and its row would be swept. A plain sink does not see it.
func TestRunnerDeliversUndecodable(t *testing.T) {
	ev := watchfeed.Event{Kind: 0, Key: "7.0", Cursor: []byte("c7"), Err: errors.New("bad wire ad")} // db.WatchUpsert

	sink := &undecodableSink{}
	r, err := NewRunner(func(context.Context) (*dbrpc.Client, func(), error) { return nil, nil, errors.New("unused") },
		Config{Source: "jobs", Src: "ap1", Constraint: `Owner == "alice"`}, sink, nil)
	if err != nil {
		t.Fatal(err)
	}
	var ver uint64
	if err := r.deliver(ev, &ver); err != nil {
		t.Fatal(err)
	}
	if len(sink.undecodable) != 1 || len(sink.kinds) != 0 {
		t.Fatalf("undecodable deliveries = %d, Apply calls = %d; want 1 and 0", len(sink.undecodable), len(sink.kinds))
	}
	c := sink.undecodable[0]
	if c.Kind != replicate.KindUpsert || c.Key != "7.0" || string(c.Cursor) != "c7" || c.Ad != nil || c.Src != "ap1" {
		t.Errorf("delivered %+v", c)
	}

	plain := &flushSink{}
	r, err = NewRunner(func(context.Context) (*dbrpc.Client, func(), error) { return nil, nil, errors.New("unused") },
		Config{Source: "jobs", Src: "ap1"}, plain, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.deliver(ev, &ver); err != nil {
		t.Fatal(err)
	}
	if _, _, _, kinds := plain.snapshot(); len(kinds) != 0 {
		t.Errorf("a plain sink was handed an undecodable change: %v", kinds)
	}
}
