// Rendering a BINARY wire row into the same CEDAR record queuewire.go builds
// from old-ClassAd text.
//
// The database can ship a matching ad two ways. As old-ClassAd text, one row per
// RPC frame -- what queuewire.go's appendRecord consumes -- or as a wire row: a
// self-contained inline-names ad, many rows per frame. The second is far cheaper
// end to end (the database never renders, and the transport stops paying a frame
// header per ad), so it is the path taken whenever the database offers it.
//
// What arrives here is therefore the ad's structure rather than its text, and the
// "Attr = Value" strings a CEDAR record is made of are produced attribute by
// attribute on this side. Two things that the text path got for free have to be
// done here instead:
//
//   - The old-ClassAd dialect. A string value must be quoted the way HTCondor's
//     OLD reader expects, which does no escape processing; rendering it for the
//     new-ClassAd reader instead doubles a backslash on every round trip through
//     a condor_q. Hence the ...Old renderer, never the plain one.
//   - The projection's reference closure. The text path asks the database for a
//     projection that also carries whatever the projected EXPRESSIONS refer to,
//     so an ad holding Requirements still arrives with RequestMemory and friends
//     and evaluates self-contained -- which is what HTCondor's own putClassAd
//     does with a projection whitelist. The wire row stream takes a plain
//     projection with no such expansion, so a projected query asks for the whole
//     ad and the closure is computed here, per ad, while rendering.
//
// collections.RenderRawAdInline is the ready-made version of the first half of
// that and is deliberately not used, for the second. It hands back rendered
// expression TEXT, and a reference closure has to read the wire NODE
// (wire.AppendNodeRefNames takes one); recovering an expression's references from
// its text would mean parsing ClassAd expressions here, per attribute, per ad. It
// also renders every entry before anything can be dropped, so a five-attribute
// projection of a forty-five-attribute job ad would render forty of them to throw
// them away. What is left after taking the walk apart is the same two public
// calls RenderRawAdInline itself makes -- Ad.ForEachNameNode and
// wire.AppendNodeTextInlineOld -- with the filter in between, so the dialect is
// still the library's decision and not a second implementation of it.
package main

import (
	"errors"
	"strings"

	"github.com/PelicanPlatform/classad/classad"
	"github.com/PelicanPlatform/classad/collections/wire"
)

// errUnreadableWireRow fails a query whose wire rows cannot be walked. See
// appendWireRecord for why this is fatal to the query rather than a skipped ad.
var errUnreadableWireRow = errors.New("the database sent an ad in a wire format this build cannot read")

// wireRequest shapes a wire-row query: what projection to ask the database for,
// and the projector that filters what comes back.
//
// A summary-only query reads nothing but the two attributes it counts by, and
// there is nothing to render. An unprojected query asks for everything and keeps
// everything. A projected one asks for everything too -- the wire-row stream has
// no reference expansion, so the whole ad is what makes the closure computable
// here -- and filters while rendering.
//
// The attributes added for counting are not in the projector, so the client never
// receives them; suppress, which does that job on the text path, has nothing left
// to drop here.
func wireRequest(attrs []string, b *adRecordBuilder) (request []string, proj *wireProjector) {
	if !b.emit {
		return attrs, nil
	}
	if len(attrs) == 0 {
		return nil, nil
	}
	wanted := attrs
	if len(b.suppress) > 0 {
		wanted = make([]string, 0, len(attrs))
		for _, a := range attrs {
			if !b.suppress[strings.ToLower(a)] {
				wanted = append(wanted, a)
			}
		}
	}
	return nil, newWireProjector(wanted)
}

// wireProjector keeps a projected query's per-ad render state: which projected
// names have been emitted for the ad in hand, and which further names its
// expressions turned out to reference. It is reused across every row of one
// query and allocates nothing per row.
//
// The reference closure is per ad because the references are: two jobs with the
// same projection can need different siblings, exactly as they can for HTCondor.
type wireProjector struct {
	// names/hashes are the caller's projection. The hash is the folded name hash
	// the stored ad's own entries are keyed by, so the common case -- an entry that
	// matches nothing -- costs one hash and no byte comparison.
	names  []string
	hashes []uint32
	done   []bool

	// Names surfaced as references by an expression already emitted for this ad,
	// and not in the projection. The name bytes alias the row, which is valid for
	// exactly as long as they are read for, so they are not copied.
	refNames  [][]byte
	refHashes []uint32
	refDone   []bool

	scratch [][]byte // per-node reference scratch, recycled

	// The type fields ship whatever the projection says, so they are not in
	// names/done and need their own emitted marks -- without them a second
	// closure pass over the same ad sends each of them twice.
	myTypeDone, targetTypeDone bool
}

// newWireProjector builds the projector for attrs, or returns nil for an empty
// projection (every attribute ships, so there is nothing to decide).
func newWireProjector(attrs []string) *wireProjector {
	if len(attrs) == 0 {
		return nil
	}
	p := &wireProjector{}
	seen := make(map[string]bool, len(attrs))
	for _, name := range attrs {
		lower := strings.ToLower(name)
		if seen[lower] {
			continue
		}
		seen[lower] = true
		p.names = append(p.names, name)
		p.hashes = append(p.hashes, wire.NameHash32(name))
	}
	if len(p.names) == 0 {
		return nil
	}
	p.done = make([]bool, len(p.names))
	return p
}

// reset clears the per-ad state.
func (p *wireProjector) reset() {
	clear(p.done)
	p.refNames, p.refHashes, p.refDone = p.refNames[:0], p.refHashes[:0], p.refDone[:0]
	p.myTypeDone, p.targetTypeDone = false, false
}

// want reports whether this entry belongs in the response, marking it emitted so
// a later pass does not repeat it. The type fields ship whatever the projection
// says, which is what the database does with one and what appendRecord does on
// the text path.
func (p *wireProjector) want(name []byte) bool {
	if wire.FoldEqualBytes(name, "MyType") {
		if p.myTypeDone {
			return false
		}
		p.myTypeDone = true
		return true
	}
	if wire.FoldEqualBytes(name, "TargetType") {
		if p.targetTypeDone {
			return false
		}
		p.targetTypeDone = true
		return true
	}
	h := wire.NameHash32Bytes(name)
	for i, wh := range p.hashes {
		if wh == h && !p.done[i] && wire.FoldEqualBytes(name, p.names[i]) {
			p.done[i] = true
			return true
		}
	}
	for i, rh := range p.refHashes {
		if rh == h && !p.refDone[i] && foldEqualNames(name, p.refNames[i]) {
			p.refDone[i] = true
			return true
		}
	}
	return false
}

// noteRef adds a name an emitted expression refers to, reporting whether it is
// new -- in which case the ad has to be walked again, since the entry holding it
// may already be behind us.
//
// A name already in the projection is not added: it is emitted, or the walk will
// reach it. A private name is never added, because a reference must not be the
// way a secret leaves a redacted response.
func (p *wireProjector) noteRef(name []byte) bool {
	h := wire.NameHash32Bytes(name)
	for i, wh := range p.hashes {
		if wh == h && wire.FoldEqualBytes(name, p.names[i]) {
			return false
		}
	}
	for i, rh := range p.refHashes {
		if rh == h && foldEqualNames(name, p.refNames[i]) {
			return false
		}
	}
	if isPrivateName(name) {
		return false
	}
	p.refNames = append(p.refNames, name)
	p.refHashes = append(p.refHashes, h)
	p.refDone = append(p.refDone, false)
	return true
}

// appendWireRecord is appendRecord for a binary wire row: it appends one ad
// record for row -- a self-contained inline-names ad -- to dst and returns the
// extended buffer.
//
// It reports false if the row cannot be walked or an expression cannot be
// rendered. That is a hard failure for the query rather than a skipped ad: this
// path is reached only against a database that offered wire rows, so a row it
// cannot read means the two sides disagree about the format, and answering a
// condor_q with silently fewer jobs would hide that.
//
// row and every name and node inside it alias the transport's frame buffer and
// are valid only for the duration of this call.
func (b *adRecordBuilder) appendWireRecord(dst []byte, row []byte) ([]byte, bool) {
	b.universe, b.status, b.haveStatus = 0, 0, false

	start := len(dst)
	if b.emit {
		dst = b.startRecord(dst)
	}
	if b.wireProj != nil {
		b.wireProj.reset()
	}

	n := 0
	good := true
	again := false
	first := true

	handle := func(name, node []byte) bool {
		// The database strips private attributes at the source (every wire-row
		// request here asks it to). Repeating the test costs one byte comparison for
		// almost every attribute, and keeps the guarantee local: nothing private
		// reaches a client even if a future server stops honouring the request.
		if isPrivateName(name) {
			return true
		}

		// Counting happens before any filtering, so a projection that leaves
		// JobStatus out of the response still produces accurate counts. Only on the
		// first pass: a later one revisits entries already counted.
		if first {
			switch {
			case wire.FoldEqualBytes(name, attrJobStatus):
				if lit, ok := wire.LiteralValue(node); ok && lit.Kind == wire.LitInt {
					b.status, b.haveStatus = int(lit.Int), true
				}
			case wire.FoldEqualBytes(name, attrJobUniverse):
				if lit, ok := wire.LiteralValue(node); ok && lit.Kind == wire.LitInt {
					b.universe = int(lit.Int)
				}
			}
		}

		if !b.emit {
			return true
		}
		if b.suppressedName(name) {
			return true
		}
		if b.wireProj != nil && !b.wireProj.want(name) {
			return true
		}

		dst = append(dst, name...)
		dst = append(dst, ' ', '=', ' ')
		var err error
		dst, err = wire.AppendNodeTextInlineOld(dst, node)
		if err != nil {
			good = false
			return false
		}
		dst = append(dst, 0)
		n++

		if b.wireProj != nil {
			b.wireProj.scratch = wire.AppendNodeRefNames(node, b.wireProj.scratch[:0])
			for _, ref := range b.wireProj.scratch {
				if b.wireProj.noteRef(ref) {
					again = true
				}
			}
		}
		return true
	}

	// Repeat while the reference closure grew: a name an expression refers to may
	// sit at an entry the walk has already passed. Bounded, because each pass
	// strictly grows the wanted set and the ad has finitely many attributes.
	for {
		again = false
		if !wire.Ad(row).ForEachNameNode(handle) || !good {
			return dst, false
		}
		first = false
		if !again {
			break
		}
	}

	if !b.emit {
		return dst, true
	}
	if b.serverTime != "" {
		dst = append(dst, b.serverTime...)
		dst = append(dst, 0)
		n++
	}
	dst = b.endRecord(dst, start, n)
	return dst, true
}

// foldEqualNames compares two raw attribute names case-insensitively, the
// bytes-against-bytes form of wire.FoldEqualBytes: a name surfaced as a
// reference arrives as row bytes, not as a string, and converting it to one to
// compare would allocate per reference per ad.
func foldEqualNames(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// isPrivateName is classad.IsPrivateAttribute over raw name bytes, gated on the
// first byte so the string conversion -- an allocation -- happens only for the
// few names that could possibly be private (the fixed set starts with c or t,
// the prefixed one with an underscore).
func isPrivateName(name []byte) bool {
	if len(name) == 0 {
		return false
	}
	switch name[0] {
	case 'c', 'C', 't', 'T', '_':
		return classad.IsPrivateAttribute(string(name))
	}
	return false
}
