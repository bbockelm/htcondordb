package main

import (
	"encoding/binary"
	"slices"
	"testing"
)

// decodeRecord walks one record out of a chunk the way the C++ consumer does, and
// returns the expression strings plus the two trailing type strings. It is the
// only check that matters for this encoding: the consumer copies these bytes to a
// CEDAR socket verbatim, so the test has to read them the way CEDAR will.
func decodeRecord(t *testing.T, chunk []byte) (exprs []string, trailing []string, rest []byte) {
	t.Helper()
	if len(chunk) < recordHeaderLen {
		t.Fatalf("chunk too short for a record header: %d bytes", len(chunk))
	}
	n := int(binary.LittleEndian.Uint32(chunk))
	payload := chunk[recordHeaderLen : recordHeaderLen+n]
	rest = chunk[recordHeaderLen+n:]

	count := int(binary.BigEndian.Uint64(payload))
	body := payload[8:]
	var strs []string
	for len(body) > 0 {
		i := 0
		for i < len(body) && body[i] != 0 {
			i++
		}
		if i == len(body) {
			t.Fatalf("unterminated string at the end of a record")
		}
		strs = append(strs, string(body[:i]))
		body = body[i+1:]
	}
	if len(strs) != count+2 {
		t.Fatalf("record holds %d strings, want %d expressions plus 2 trailing types", len(strs), count)
	}
	return strs[:count], strs[count:], rest
}

func TestAppendRecordLayout(t *testing.T) {
	b := &adRecordBuilder{emit: true}
	chunk := b.appendRecord(nil, "ClusterId = 12\nProcId = 0\nOwner = \"alice\"")

	exprs, trailing, rest := decodeRecord(t, chunk)
	if len(rest) != 0 {
		t.Fatalf("one record left %d trailing bytes", len(rest))
	}
	want := []string{"ClusterId = 12", "ProcId = 0", `Owner = "alice"`}
	if len(exprs) != len(want) {
		t.Fatalf("got %d expressions %q, want %d", len(exprs), exprs, len(want))
	}
	for i := range want {
		if exprs[i] != want[i] {
			t.Errorf("expression %d = %q, want %q", i, exprs[i], want[i])
		}
	}
	// HTCondor always sends the trailing MyType/TargetType strings empty.
	if trailing[0] != "" || trailing[1] != "" {
		t.Errorf("trailing type strings = %q, want two empty strings", trailing)
	}
}

func TestAppendRecordDropsPrivateAttributes(t *testing.T) {
	b := &adRecordBuilder{emit: true}
	// ClaimId is private (V1); _condor_priv_x is private (V2). Both must be gone,
	// which is what the schedd's PUT_CLASSAD_NO_PRIVATE does.
	chunk := b.appendRecord(nil, "ClusterId = 1\nClaimId = \"secret\"\n_condor_priv_x = 3\nProcId = 2")
	exprs, _, _ := decodeRecord(t, chunk)
	if len(exprs) != 2 {
		t.Fatalf("got %q, want only the two public attributes", exprs)
	}
	for _, e := range exprs {
		if e == `ClaimId = "secret"` || e == "_condor_priv_x = 3" {
			t.Errorf("private attribute %q survived", e)
		}
	}
}

func TestAppendRecordServerTimeAndProjection(t *testing.T) {
	b := &adRecordBuilder{
		emit:       true,
		serverTime: serverTimeExpr(1700000000),
		projection: lowerSet([]string{"clusterid", "MyType"}),
	}
	chunk := b.appendRecord(nil, "ClusterId = 1\nOwner = \"bob\"\nMyType = \"Job\"")
	exprs, _, _ := decodeRecord(t, chunk)

	got := map[string]bool{}
	for _, e := range exprs {
		got[e] = true
	}
	if !got["ClusterId = 1"] {
		t.Errorf("projected attribute missing from %q", exprs)
	}
	if got[`Owner = "bob"`] {
		t.Errorf("unprojected attribute survived in %q", exprs)
	}
	// A projection never strips the type attributes, matching putClassAd's
	// whitelist handling.
	if !got[`MyType = "Job"`] {
		t.Errorf("MyType was projected away: %q", exprs)
	}
	if !got["ServerTime = 1700000000"] {
		t.Errorf("ServerTime missing from %q", exprs)
	}
}

func TestAppendRecordConcatenates(t *testing.T) {
	b := &adRecordBuilder{emit: true}
	chunk := b.appendRecord(nil, "A = 1")
	chunk = b.appendRecord(chunk, "B = 2\nC = 3")

	first, _, rest := decodeRecord(t, chunk)
	if len(first) != 1 || first[0] != "A = 1" {
		t.Fatalf("first record = %q", first)
	}
	second, _, rest := decodeRecord(t, rest)
	if len(second) != 2 {
		t.Fatalf("second record = %q", second)
	}
	if len(rest) != 0 {
		t.Fatalf("%d bytes left after two records", len(rest))
	}
}

func TestAppendRecordEmptyAd(t *testing.T) {
	// An ad whose every attribute was redacted or projected away is still a valid
	// ad on the wire: zero expressions and the two trailing strings. Dropping it
	// would make the ad count disagree with the aggregate's count.
	b := &adRecordBuilder{emit: true}
	chunk := b.appendRecord(nil, "")
	exprs, trailing, rest := decodeRecord(t, chunk)
	if len(exprs) != 0 || len(trailing) != 2 || len(rest) != 0 {
		t.Fatalf("empty ad encoded as %q / %q with %d bytes left", exprs, trailing, len(rest))
	}
}

func TestSplitList(t *testing.T) {
	got := splitList(" ClusterId , ProcId ,, Owner ")
	want := []string{"ClusterId", "ProcId", "Owner"}
	if len(got) != len(want) {
		t.Fatalf("got %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
	if splitList("  ") != nil {
		t.Errorf("an empty list should parse to nil")
	}
}

func TestAppendRecordCounts(t *testing.T) {
	b := &adRecordBuilder{emit: true}
	b.appendRecord(nil, "ClusterId = 1\nJobStatus = 2\nJobUniverse = 5")
	if !b.haveStatus || b.status != 2 || b.universe != 5 {
		t.Fatalf("counted (universe=%d status=%d have=%v), want (5, 2, true)", b.universe, b.status, b.haveStatus)
	}

	// An ad with no integer JobStatus cannot be bucketed. The schedd's own counter
	// declines to place it too, so it must not be counted as status 0.
	b.appendRecord(nil, "ClusterId = 2\nJobUniverse = 5")
	if b.haveStatus {
		t.Errorf("an ad with no JobStatus was counted")
	}
	b.appendRecord(nil, "JobStatus = undefined")
	if b.haveStatus {
		t.Errorf("an ad with a non-integer JobStatus was counted")
	}

	// The per-row outputs must not carry over from the previous row.
	b.appendRecord(nil, "JobStatus = 1\nJobUniverse = 7")
	b.appendRecord(nil, "ClusterId = 3")
	if b.haveStatus || b.universe != 0 {
		t.Errorf("row state leaked: universe=%d have=%v", b.universe, b.haveStatus)
	}
}

func TestWithCountingAttrs(t *testing.T) {
	// A projection that does not name them gets them added for counting and
	// marked for removal on the way out.
	req, suppress := withCountingAttrs([]string{"ClusterId", "ProcId"})
	if len(req) != 4 {
		t.Fatalf("request = %q, want the projection plus the two counting attributes", req)
	}
	if !slices.Contains(suppress, "jobstatus") || !slices.Contains(suppress, "jobuniverse") {
		t.Fatalf("suppress = %v, want both counting attributes", suppress)
	}

	// One the client did ask for must be returned to it, so only the other is
	// suppressed.
	req, suppress = withCountingAttrs([]string{"ClusterId", "jobstatus"})
	if len(req) != 3 {
		t.Fatalf("request = %q, want one attribute added", req)
	}
	if slices.Contains(suppress, "jobstatus") {
		t.Errorf("an attribute the client projected was suppressed")
	}
	if !slices.Contains(suppress, "jobuniverse") {
		t.Errorf("suppress = %v, want JobUniverse", suppress)
	}

	// No projection means every attribute: nothing to add, nothing to drop.
	req, suppress = withCountingAttrs(nil)
	if req != nil || suppress != nil {
		t.Errorf("an empty projection produced %q / %v", req, suppress)
	}
}

func TestAppendRecordSuppressesCountingAttrs(t *testing.T) {
	attrs := []string{"ClusterId"}
	_, suppress := withCountingAttrs(attrs)
	b := &adRecordBuilder{emit: true, suppress: suppress}
	chunk := b.appendRecord(nil, "ClusterId = 1\nJobStatus = 2\nJobUniverse = 5")

	exprs, _, _ := decodeRecord(t, chunk)
	if len(exprs) != 1 || exprs[0] != "ClusterId = 1" {
		t.Fatalf("got %q, want only the projected attribute", exprs)
	}
	// Suppressed on the wire, still counted.
	if !b.haveStatus || b.status != 2 || b.universe != 5 {
		t.Errorf("suppressed attributes were not counted: universe=%d status=%d have=%v",
			b.universe, b.status, b.haveStatus)
	}
}

func TestAppendRecordSummaryOnlyEmitsNothing(t *testing.T) {
	b := &adRecordBuilder{emit: false}
	chunk := b.appendRecord(nil, "ClusterId = 1\nJobStatus = 4\nJobUniverse = 1")
	if len(chunk) != 0 {
		t.Fatalf("a summary-only query produced %d bytes", len(chunk))
	}
	if !b.haveStatus || b.status != 4 || b.universe != 1 {
		t.Errorf("counted (universe=%d status=%d have=%v), want (1, 4, true)", b.universe, b.status, b.haveStatus)
	}
}
