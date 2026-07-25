package opensearchsync

import (
	"strings"
	"testing"

	"github.com/PelicanPlatform/classad/classad"
)

const testLaunch int64 = 1_800_000_000

func mustAd(t *testing.T, src string) *classad.ClassAd {
	t.Helper()
	ad, err := classad.Parse(src)
	if err != nil {
		t.Fatalf("parse %q: %v", src, err)
	}
	return ad
}

func transform(t *testing.T, src string) (string, map[string]any, bool) {
	t.Helper()
	return NewTransformer(testLaunch).Transform(mustAd(t, src))
}

// TestTransformCompletedJob covers the derived fields, doc id, and the main type coercions on a
// realistic completed job.
func TestTransformCompletedJob(t *testing.T) {
	id, doc, ok := transform(t, `[
		GlobalJobId = "ap40.chtc.wisc.edu#12.0#1700000000";
		JobStatus = 4; JobUniverse = 5;
		CompletionDate = 1700000100; QDate = 1699999000;
		Owner = "alice"; RemoteHost = "slot1_3@node7.example.com";
		RequestCpus = 4; ExitCode = 0; RemoteWallClockTime = 3600
	]`)
	if !ok {
		t.Fatal("Transform returned ok=false for a valid job")
	}
	if id != "ap40.chtc.wisc.edu#12.0#1700000000#1700000100" {
		t.Errorf("doc id = %q", id)
	}
	want := map[string]any{
		"RecordTime":          int64(1700000100), // terminal status + CompletionDate
		"ScheddName":          "ap40.chtc.wisc.edu",
		"StartdSlot":          "slot1_3",
		"StartdName":          "node7.example.com",
		"Status":              "Completed",
		"Universe":            "Vanilla",
		"Owner":               "alice",           // INDEXED_KEYWORD -> string
		"JobStatus":           int64(4),          // INT
		"RequestCpus":         int64(4),          // Request prefix -> int
		"RemoteWallClockTime": int64(3600),       // INT
		"QDate":               int64(1699999000), // DATE -> epoch int
	}
	for k, v := range want {
		if doc[k] != v {
			t.Errorf("doc[%q] = %#v (%T), want %#v (%T)", k, doc[k], doc[k], v, v)
		}
	}
}

func TestRecordTimeFallback(t *testing.T) {
	// Non-terminal (Idle): CompletionDate ignored; EpochWriteDate wins over EnteredCurrentStatus.
	_, doc, _ := transform(t, `[ GlobalJobId="s#1.0#1"; JobStatus=1; CompletionDate=1700000100; EpochWriteDate=1699999800; EnteredCurrentStatus=1699999500 ]`)
	if doc["RecordTime"] != int64(1699999800) {
		t.Errorf("RecordTime = %v, want EpochWriteDate 1699999800", doc["RecordTime"])
	}
	// Only EnteredCurrentStatus present.
	_, doc, _ = transform(t, `[ GlobalJobId="s#1.0#1"; JobStatus=1; EnteredCurrentStatus=1699999500 ]`)
	if doc["RecordTime"] != int64(1699999500) {
		t.Errorf("RecordTime = %v, want EnteredCurrentStatus 1699999500", doc["RecordTime"])
	}
	// Nothing usable -> process launch time.
	_, doc, _ = transform(t, `[ GlobalJobId="s#1.0#1"; JobStatus=1 ]`)
	if doc["RecordTime"] != testLaunch {
		t.Errorf("RecordTime = %v, want launch %v", doc["RecordTime"], testLaunch)
	}
}

func TestTransformSkips(t *testing.T) {
	if _, _, ok := transform(t, `[ GlobalJobId="s#1.0#1"; JobStatus=4; TaskType="ROOT" ]`); ok {
		t.Error("DAG root (TaskType=ROOT) should be skipped")
	}
	if _, _, ok := transform(t, `[ GlobalJobId="s#1.0#1" ]`); ok {
		t.Error("ad missing JobStatus should be skipped")
	}
	if _, _, ok := transform(t, `[ JobStatus=4 ]`); ok {
		t.Error("ad missing GlobalJobId should be skipped")
	}
}

func TestRedaction(t *testing.T) {
	_, doc, ok := transform(t, `[ GlobalJobId="s#1.0#1"; JobStatus=4; Owner="bob"; ClaimId="secret-claim"; Environment="PATH=/x"; EC2SecretAccessKey="AKIA..." ]`)
	if !ok {
		t.Fatal("ok=false")
	}
	for _, secret := range []string{"ClaimId", "Environment", "EC2SecretAccessKey"} {
		if _, present := doc[secret]; present {
			t.Errorf("redacted attr %q leaked into the document", secret)
		}
	}
	if doc["Owner"] != "bob" {
		t.Errorf("Owner = %v, want bob", doc["Owner"])
	}
}

func TestZeroDateDropped(t *testing.T) {
	_, doc, _ := transform(t, `[ GlobalJobId="s#1.0#1"; JobStatus=4; QDate=0; CompletionDate=1700000100 ]`)
	if _, present := doc["QDate"]; present {
		t.Errorf("zero-valued QDate should be dropped, got %v", doc["QDate"])
	}
}

func TestCaseNormalizeAndAutoTypes(t *testing.T) {
	_, doc, _ := transform(t, `[
		GlobalJobId="s#1.0#1"; JobStatus=4;
		MyCustomThing = "hi";
		SomethingProvisioned = 7;
		RequestGizmos = 5;
		WantFoo = true;
		ArbitraryDate = 1700
	]`)
	if v, ok := doc["mycustomthing"]; !ok || v != "hi" { // unknown -> lowercased, string
		t.Errorf("unknown attr not lowercased/stringified: %v", doc)
	}
	if doc["SomethingProvisioned"] != int64(7) { // suffix Provisioned -> int
		t.Errorf("SomethingProvisioned = %#v, want int 7", doc["SomethingProvisioned"])
	}
	if doc["RequestGizmos"] != int64(5) { // Request prefix -> int
		t.Errorf("RequestGizmos = %#v, want int 5", doc["RequestGizmos"])
	}
	if doc["WantFoo"] != true { // Want prefix -> bool
		t.Errorf("WantFoo = %#v, want bool true", doc["WantFoo"])
	}
	if doc["ArbitraryDate"] != int64(1700) { // suffix Date -> epoch int
		t.Errorf("ArbitraryDate = %#v, want int 1700", doc["ArbitraryDate"])
	}
}

func TestExprAndStringFallbacks(t *testing.T) {
	// Undefined-referencing expression -> "<key>_EXPR" holding the raw expression text.
	_, doc, _ := transform(t, `[ GlobalJobId="s#1.0#1"; JobStatus=4; Broken = DoesNotExist + 5 ]`)
	expr, ok := doc["broken_EXPR"].(string) // "Broken" unknown -> "broken"
	if !ok || !strings.Contains(expr, "DoesNotExist") {
		t.Errorf("broken_EXPR = %#v, want raw expression containing DoesNotExist", doc["broken_EXPR"])
	}
	// Non-numeric value in an int-category attr -> "<key>_STRING".
	_, doc, _ = transform(t, `[ GlobalJobId="s#1.0#1"; JobStatus=4; ExitCode = "notanumber" ]`)
	if doc["ExitCode_STRING"] != "notanumber" {
		t.Errorf("ExitCode_STRING = %#v, want \"notanumber\"", doc["ExitCode_STRING"])
	}
	if _, present := doc["ExitCode"]; present {
		t.Errorf("ExitCode should not be present as int when coercion failed")
	}
}

func TestStringTruncation(t *testing.T) {
	long := strings.Repeat("x", 300)
	_, doc, _ := transform(t, `[ GlobalJobId="s#1.0#1"; JobStatus=4; Owner="`+long+`" ]`)
	got, _ := doc["Owner"].(string)
	if len(got) != 256 || !strings.HasSuffix(got, "...") {
		t.Errorf("Owner truncation: len=%d suffix=%q, want len 256 ending ...", len(got), got[max(0, len(got)-3):])
	}
}
