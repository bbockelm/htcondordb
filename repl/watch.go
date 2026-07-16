package repl

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/db"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// Watch event kinds on the wire (dbrpc.WatchEvent.Kind).
const (
	watchUpsert = 0
	watchDelete = 1
	watchReset  = 2
)

// runWatch streams live changes to a table until the user interrupts it (Ctrl+C), the
// connection drops, or the LIMIT is reached. It projects each event per the WATCH
// projection, filters upserts by the WHERE constraint (deletes are always shown), and --
// unless SINCE BEGINNING -- drops the initial replay so only changes from now on appear.
func (s *session) runWatch(ctx context.Context, console io.Writer, st *Statement) {
	var filter *db.Constraint
	if st.Where != "" {
		f, err := db.ParseConstraint(st.Where)
		if err != nil {
			fmt.Fprintf(console, "error: bad WHERE constraint: %v\n", err)
			return
		}
		filter = f
	}
	events, stop, err := s.exec.WatchStream(st.Table)
	if err != nil {
		fmt.Fprintf(console, "error: %v\n", err)
		if h := HintFor(err); h != "" {
			fmt.Fprintf(console, "  hint: %s\n", h)
		}
		return
	}
	defer stop() // cancels the server-side watch on return (Ctrl+C, EOF, LIMIT, error)

	// Ctrl+C interrupts just the watch, returning to the prompt.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	defer signal.Stop(sigCh)

	beginning := st.Since == "beginning"
	if beginning {
		fmt.Fprintf(console, "# replaying %s from the beginning, then watching for changes (Ctrl+C to stop)\n", st.Table)
	} else {
		fmt.Fprintf(console, "# watching %s for changes (Ctrl+C to stop)\n", st.Table)
	}
	s.watchHeader(console, st)

	live := beginning // NOW (false): drop events until the initial replay's synced marker
	count := 0
	start := time.Now()
	finish := func(why string) {
		fmt.Fprintf(console, "# %s -- %d event(s) in %s\n", why, count, time.Since(start).Round(time.Millisecond))
	}
	for {
		select {
		case <-ctx.Done():
			finish("watch cancelled")
			return
		case <-sigCh:
			finish("watch stopped")
			return
		case ev, ok := <-events:
			if !ok {
				finish("watch stream closed")
				return
			}
			// The synced marker (an upsert with no key, carrying the cursor) ends the
			// initial replay: after it, everything is a live change.
			if ev.Kind == watchUpsert && ev.Key == "" {
				if !live {
					live = true
					if beginning {
						fmt.Fprintln(console, "# --- now live ---")
					}
				}
				continue
			}
			if ev.Kind == watchReset {
				continue // handled by the SINCE semantics above; nothing to print
			}
			if !live {
				continue // NOW mode: still in the replay we are dropping
			}
			if !s.watchEmit(console, st, filter, ev) {
				continue // filtered out
			}
			count++
			if st.Limit > 0 && count >= st.Limit {
				finish("watch limit reached")
				return
			}
		}
	}
}

// watchHeader prints the column header for an explicit projection ("*" is self-describing
// per row, so it gets no fixed header).
func (s *session) watchHeader(console io.Writer, st *Statement) {
	if len(st.Items) == 1 && st.Items[0].Star {
		return
	}
	cols := make([]string, 0, len(st.Items)+2)
	cols = append(cols, "event", "Key")
	for _, it := range st.Items {
		cols = append(cols, it.header())
	}
	fmt.Fprintln(console, watchRow(cols))
}

// watchEmit renders one event; it returns false when the event is filtered out. Upserts
// are parsed, matched against the WHERE constraint, and projected; deletes are always
// shown (a deleted ad has no attributes to test or project).
func (s *session) watchEmit(console io.Writer, st *Statement, filter *db.Constraint, ev dbrpc.WatchEvent) bool {
	kind := "UPSERT"
	if ev.Kind == watchDelete {
		kind = "DELETE"
	}
	star := len(st.Items) == 1 && st.Items[0].Star

	if ev.Kind == watchDelete {
		if star {
			fmt.Fprintf(console, "%-6s %s\n", kind, ev.Key)
		} else {
			cells := make([]string, len(st.Items)+2)
			cells[0], cells[1] = kind, ev.Key
			fmt.Fprintln(console, watchRow(cells))
		}
		return true
	}

	ad, err := classad.Parse(ev.AdText)
	if err != nil {
		fmt.Fprintf(console, "%-6s %s  (unparseable ad: %v)\n", kind, ev.Key, err)
		return true
	}
	if filter != nil && !filter.Matches(ad) {
		return false
	}
	if star {
		fmt.Fprintf(console, "%-6s %-24s %s\n", kind, ev.Key, strings.TrimSpace(ad.String()))
		return true
	}
	cells := make([]string, 0, len(st.Items)+2)
	cells = append(cells, kind, ev.Key)
	for _, it := range st.Items {
		cells = append(cells, valueDisplay(ad.EvaluateAttr(it.header())))
	}
	fmt.Fprintln(console, watchRow(cells))
	return true
}

// watchRow formats a streaming row with fixed-ish column widths (event, key, then values).
func watchRow(cells []string) string {
	var b strings.Builder
	for i, c := range cells {
		w := 14
		switch i {
		case 0:
			w = 6 // event
		case 1:
			w = 24 // key
		}
		if i > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%-*s", w, c)
	}
	return strings.TrimRight(b.String(), " ")
}
