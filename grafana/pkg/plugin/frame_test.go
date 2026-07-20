package plugin

import (
	"testing"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/data"

	"github.com/bbockelm/htcondordb/repl"
)

func TestResultToFrame_TypeInference(t *testing.T) {
	res := &repl.Result{
		IsSelect: true,
		Columns:  []string{"Owner", "QDate", "Cpus"},
		Rows: [][]string{
			{"alice", "1609459200", "8"},
			{"bob", "1609462800", "16"},
		},
	}
	frame := resultToFrame("A", res, "", "")
	if len(frame.Fields) != 3 {
		t.Fatalf("got %d fields, want 3", len(frame.Fields))
	}

	// Owner -> string
	if frame.Fields[0].Type() != data.FieldTypeNullableString {
		t.Errorf("Owner field type = %v, want nullable string", frame.Fields[0].Type())
	}
	// QDate -> time (known time attr)
	if frame.Fields[1].Type() != data.FieldTypeNullableTime {
		t.Errorf("QDate field type = %v, want nullable time", frame.Fields[1].Type())
	}
	if tv, ok := frame.Fields[1].At(0).(*time.Time); !ok || tv == nil || tv.Unix() != 1609459200 {
		t.Errorf("QDate[0] = %v, want unix 1609459200", frame.Fields[1].At(0))
	}
	// Cpus -> number
	if frame.Fields[2].Type() != data.FieldTypeNullableFloat64 {
		t.Errorf("Cpus field type = %v, want nullable float64", frame.Fields[2].Type())
	}
	if v, ok := frame.Fields[2].At(1).(*float64); !ok || v == nil || *v != 16 {
		t.Errorf("Cpus[1] = %v, want 16", frame.Fields[2].At(1))
	}
}

func TestResultToFrame_TimeFieldOverride(t *testing.T) {
	// A column not in knownTimeAttrs still becomes a time field when the builder
	// names it as the TimeField.
	res := &repl.Result{
		IsSelect: true,
		Columns:  []string{"ts", "n"},
		Rows:     [][]string{{"1609459200", "5"}},
	}
	frame := resultToFrame("A", res, "ts", "timeseries")
	if frame.Fields[0].Type() != data.FieldTypeNullableTime {
		t.Errorf("ts field type = %v, want nullable time (TimeField override)", frame.Fields[0].Type())
	}
	if frame.Meta == nil || frame.Meta.PreferredVisualization != data.VisTypeGraph {
		t.Errorf("expected graph visualization hint for timeseries format")
	}
}

func TestResultToFrame_MislabeledTimeFallsBack(t *testing.T) {
	// A column named like a time attr but holding non-numeric text must not become
	// a broken time field.
	res := &repl.Result{
		IsSelect: true,
		Columns:  []string{"QDate"},
		Rows:     [][]string{{"not-a-number"}},
	}
	frame := resultToFrame("A", res, "", "")
	if frame.Fields[0].Type() != data.FieldTypeNullableString {
		t.Errorf("mislabeled QDate type = %v, want nullable string fallback", frame.Fields[0].Type())
	}
}

func TestResultToFrame_UndefinedIsNullNumber(t *testing.T) {
	res := &repl.Result{
		IsSelect: true,
		Columns:  []string{"Cpus"},
		Rows:     [][]string{{"8"}, {"undefined"}},
	}
	frame := resultToFrame("A", res, "", "")
	if frame.Fields[0].Type() != data.FieldTypeNullableFloat64 {
		t.Fatalf("Cpus type = %v, want nullable float64", frame.Fields[0].Type())
	}
	if v := frame.Fields[0].At(1).(*float64); v != nil {
		t.Errorf("Cpus[1] = %v, want nil for undefined", *v)
	}
}
