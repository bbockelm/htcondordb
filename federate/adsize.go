package federate

import (
	"github.com/PelicanPlatform/classad/ast"
	"github.com/PelicanPlatform/classad/classad"
)

// Per-node costs for approxAdBytes. They are the shape of the decoded ad in Go, not of its wire
// form: an attribute is a *ast.AttributeAssignment (pointer in the slice, two-word struct, a string
// header and an interface word) plus the expression node it points at, each its own allocation
// rounded up to a size class, plus its entry in the ad's name index. Against parsed ads of three
// shapes they land 4-14% HIGH of what the heap really grows by (TestApproxAdBytesTracksTheHeap),
// which is the direction to be wrong in and as close as the batch budget needs: it decides when to
// stop buffering, not what to allocate.
const (
	attrNodeBytes = 96 // the assignment node, its slot in the slice, the name's header and its index entry
	// adBaseBytes is what an ad costs before its first attribute: the ClassAd struct, the slice
	// header and the attribute index's own allocation.
	adBaseBytes   = 512
	exprNodeBytes = 24 // a literal node (one allocation, one word of payload)
	// deepExprBytes stands in for an expression this walk does not descend into: an operator tree,
	// a function call, a reference. Archive records are written by the schedd as literals, so these
	// are rare and small; a record built of large unevaluated expressions is under-counted.
	deepExprBytes = 64
	// maxExprDepth bounds the walk. A record nested deeper than the archive can store is refused at
	// append (db.ErrTooDeep), so nothing this deep is worth measuring exactly, and a bound keeps a
	// hostile ad from recursing the daemon's stack away.
	maxExprDepth = 8
)

// approxAdBytes estimates the heap an ad holds down while it sits in a catch-up batch.
//
// It walks the ad's top-level attributes and their values (ClassAd.AST() hands over the attribute
// slice without serialising anything) and adds a fixed cost per node plus the length of every
// string it finds. The ads in a batch are not copies: the stream allocated them to deliver them,
// and buffering only keeps them alive past the point where they would otherwise be collected, so
// what this measures -- the nodes reachable from the ad -- is what the batch actually costs.
//
// Ways it is wrong, all of them bounded and none of them silent:
//
//   - the per-node constants are one size for every node, so an ad of many tiny integer attributes
//     is over-counted and one of a few huge strings is dominated by the string lengths, which are
//     exact;
//   - an expression that is not a literal, a list or a nested record counts as deepExprBytes
//     whatever it holds, and anything below maxExprDepth counts as one node;
//   - it ignores the Go map a ClassAd keeps to index its attributes by name, which is roughly
//     another 50 bytes per attribute on an owned ad.
//
// Under-counting makes a batch hold more than the budget says, so the constants are set a little
// high rather than low. The budget bounds an order of magnitude; it is not an allocator.
func approxAdBytes(ad *classad.ClassAd) int {
	a := ad.AST()
	if a == nil {
		return 0
	}
	return adBaseBytes + approxAttrBytes(a, 0)
}

func approxAttrBytes(a *ast.ClassAd, depth int) int {
	if a == nil {
		return 0
	}
	n := 0
	for _, attr := range a.Attributes {
		n += attrNodeBytes + len(attr.Name) + approxExprBytes(attr.Value, depth)
	}
	return n
}

func approxExprBytes(e ast.Expr, depth int) int {
	if e == nil {
		return 0
	}
	if depth >= maxExprDepth {
		return deepExprBytes
	}
	switch v := e.(type) {
	case *ast.StringLiteral:
		return exprNodeBytes + len(v.Value)
	case *ast.IntegerLiteral, *ast.RealLiteral, *ast.BooleanLiteral, *ast.UndefinedLiteral, *ast.ErrorLiteral:
		return exprNodeBytes
	case *ast.ListLiteral:
		n := exprNodeBytes
		for _, el := range v.Elements {
			n += approxExprBytes(el, depth+1)
		}
		return n
	case *ast.ClassAd:
		return exprNodeBytes + approxAttrBytes(v, depth+1)
	case *ast.RecordLiteral:
		return exprNodeBytes + approxAttrBytes(v.ClassAd, depth+1)
	default:
		return deepExprBytes
	}
}
