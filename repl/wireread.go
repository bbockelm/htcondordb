package repl

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/PelicanPlatform/classad/ast"
	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections/vm"
	"github.com/PelicanPlatform/classad/collections/wire"
	"github.com/PelicanPlatform/classad/dbrpc"
)

// Reading rows over the WIRE-FORM relay rather than as old-ClassAd text.
//
// The text path renders every matching row to text at the server and parses it back here.
// Both halves are pure overhead between two processes that speak the same encoding: the row
// leaves storage as wire bytes and is rebuilt into an AST at this end, so rendering it to
// text in between only costs a render and a parse. Per row, decoding wire instead of parsing
// text is ~6x faster and ~40% of the allocations (see BenchmarkRowDecode); at 20k rows that
// is the difference between the parse dominating a query and being a rounding error.
//
// Two things make this a fallback-shaped change rather than a swap:
//
//   - Only a persistent table can serve wire rows, and an older server does not know the
//     opcode at all. Both answer dbrpc.ErrRawWireUnsupported BEFORE the first row (the
//     server refuses rather than streaming an empty result, which would be indistinguishable
//     from "nothing matched"), so the caller re-runs on the text path.
//
//   - The wire relay projects to exactly the named attributes; the text path asks the server
//     to chase references first, so a projected attribute holding an expression over its
//     siblings still evaluates. errWireRefsIncomplete below detects when that difference
//     could matter and re-runs the query on the text path, so the fast path never answers
//     from a row it cannot fully evaluate.

// errWireRefsIncomplete reports that a projected wire row holds an expression this executor
// cannot evaluate from the row alone -- it reads a sibling the projection dropped, or it
// calls eval(), whose reads are not statically known. The query re-runs on the ref-chasing
// text path. In practice job and history display columns are literal-valued, so this is the
// rare case, but answering from an incomplete row would silently evaluate to undefined.
var errWireRefsIncomplete = errors.New("repl: projected wire row is missing a referenced attribute")

// queryAdsWire fetches matching rows as wire-form rows and decodes them here, with no
// old-ClassAd render or parse in between. attrs restricts the projection (nil = whole ads,
// which is always self-contained). It returns dbrpc.ErrRawWireUnsupported or
// errWireRefsIncomplete when the caller should fall back to the text path.
func (e *Executor) queryAdsWire(table, where string, attrs []string, limit int) ([]*classad.ClassAd, error) {
	var ads []*classad.ClassAd
	var rowErr error
	// redact=false leaves the private-attribute decision to the connection's privilege,
	// exactly as the text row streams do.
	err := e.c.QueryRawWireStream(context.Background(), table, constraint(where), attrs, limit, false,
		func(row []byte) bool {
			// The row aliases the transport's frame buffer and is only valid until this
			// returns; the decode copies everything it keeps.
			node, derr := wire.DecodeInline(row)
			if derr != nil {
				rowErr = fmt.Errorf("decoding a wire row: %w", derr)
				return false
			}
			if len(attrs) > 0 && !selfContained(node) {
				rowErr = errWireRefsIncomplete
				return false
			}
			ads = append(ads, classad.FromAST(node))
			return true
		})
	if rowErr != nil {
		return nil, rowErr
	}
	if err != nil {
		return nil, err
	}
	return ads, nil
}

// selfContained reports whether every attribute of a projected row can be evaluated from
// that row alone: a literal always can, and an expression can only if every attribute it
// reads is also present. An expression calling eval() never can -- its reads are not
// statically known -- so it counts as incomplete.
func selfContained(ad *ast.ClassAd) bool {
	if ad == nil {
		return true
	}
	present := make(map[string]bool, len(ad.Attributes))
	for _, a := range ad.Attributes {
		present[strings.ToLower(a.Name)] = true
	}
	for _, a := range ad.Attributes {
		refs, safe := vm.SelfRefsSafe(a.Value)
		if !safe {
			return false
		}
		for _, r := range refs {
			if !present[strings.ToLower(r)] {
				return false
			}
		}
	}
	return true
}

// wireEligible reports whether a read against this table can try the wire relay at all.
//
// An archive is served by its own opcode, not the table row stream, so the wire op would
// fail as an unknown table rather than fall back cleanly. And inside a transaction every
// read must go through the transaction, which has no wire variant: the connection-level
// op reads the committed store, so it would miss the transaction's own writes and report
// them as absent rather than fail.
func (e *Executor) wireEligible(table string) bool {
	return !e.wireRowsOff && !e.isArchive(table) && !e.txReads(table)
}

// wireFallback reports whether err means "this read cannot use the wire relay" -- as opposed
// to a real failure -- so the caller should re-run it on the text path.
func wireFallback(err error) bool {
	return errors.Is(err, dbrpc.ErrRawWireUnsupported) || errors.Is(err, errWireRefsIncomplete)
}
