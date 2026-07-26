package opensearchsync

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/PelicanPlatform/classad/classad"
)

// This file is a faithful Go port of condor_adstash's convert.py: it turns a job ClassAd into
// the flat JSON document adstash indexes into Elasticsearch/OpenSearch, applying the same
// attribute-name normalization, per-category type coercion, redaction, and derived fields. The
// attribute category tables live in attrs.go (generated from convert.py). Keeping the transform
// byte-compatible with adstash means documents from this sync and from a legacy adstash land in
// the same index shape.

// attrSet is a case-preserving membership set of attribute names.
type attrSet map[string]struct{}

func newAttrSet(names []string) attrSet {
	s := make(attrSet, len(names))
	for _, n := range names {
		s[n] = struct{}{}
	}
	return s
}

func (s attrSet) has(name string) bool { _, ok := s[name]; return ok }

var (
	indexedKeywordSet = newAttrSet(indexedKeywordAttrs)
	noindexKeywordSet = newAttrSet(noindexKeywordAttrs)
	floatSet          = newAttrSet(floatAttrs)
	intSet            = newAttrSet(intAttrs)
	dateSet           = newAttrSet(dateAttrs)
	boolSet           = newAttrSet(boolAttrs)
	nestedSet         = newAttrSet(nestedAttrs)
	ignoreSet         = newAttrSet(ignoreAttrs)

	// knownAttrsMap maps a casefolded attribute name to its canonical casing, mirroring
	// convert.py's KNOWN_ATTRS_MAP. Built from every category table.
	knownAttrsMap = buildKnownAttrsMap()
)

func buildKnownAttrsMap() map[string]string {
	m := map[string]string{}
	add := func(names []string) {
		for _, n := range names {
			m[strings.ToLower(n)] = n
		}
	}
	// NB: REQUIRED_ATTRS is deliberately NOT part of KNOWN_ATTRS in convert.py — only the
	// type-category tables are. (Required attrs that carry a type are already in one of those.)
	add(indexedKeywordAttrs)
	add(noindexKeywordAttrs)
	add(floatAttrs)
	add(intAttrs)
	add(dateAttrs)
	add(boolAttrs)
	add(nestedAttrs)
	add(ignoreAttrs)
	return m
}

// AUTO_ATTRS regexes from convert.py:574 — used by caseNormalize to classify unknown attrs by
// suffix/prefix and CamelCase-join the captured groups.
var (
	reDate        = regexp.MustCompile(`^(.*)(Date)$`)
	reProvisioned = regexp.MustCompile(`^(.*)(Provisioned)$`)
	reRequest     = regexp.MustCompile(`^(Request)([A-Za-df-z].*)$`) // excludes Requested* (no 'e')
	reWantHasIs   = regexp.MustCompile(`(?i)^(Want|Has|Is)([A-Z_].*)$`)
)

// caseNormalize canonicalizes an attribute name to the ES/OpenSearch field name, porting
// convert.py's case_normalize: exact-known kept; casefold-known mapped to canonical casing;
// otherwise classified by the AUTO_ATTRS regexes (CamelCase-joining the groups); else lowered.
func caseNormalize(attr string) string {
	if _, ok := knownAttrsMap[strings.ToLower(attr)]; ok && knownAttrsMap[strings.ToLower(attr)] == attr {
		return attr // exact known
	}
	if canon, ok := knownAttrsMap[strings.ToLower(attr)]; ok {
		return canon // casefold-known -> canonical casing
	}
	for _, re := range []*regexp.Regexp{reDate, reProvisioned, reRequest, reWantHasIs} {
		if m := re.FindStringSubmatch(attr); m != nil {
			return capitalizeJoin(m[1:])
		}
	}
	return strings.ToLower(attr)
}

// capitalizeJoin joins regex groups with Python's str.capitalize semantics (first char upper,
// rest lower) — matching convert.py's "".join(x.capitalize() for x in match.groups()).
func capitalizeJoin(groups []string) string {
	var b strings.Builder
	for _, g := range groups {
		if g == "" {
			continue
		}
		b.WriteString(strings.ToUpper(g[:1]))
		b.WriteString(strings.ToLower(g[1:]))
	}
	return b.String()
}

// Transformer turns job ClassAds into adstash-shaped documents. launchTime is the process
// start time (unix seconds) used as record_time's last-resort fallback, matching adstash's
// module-level _LAUNCH_TIME constant.
type Transformer struct {
	launchTime int64
}

// NewTransformer builds a Transformer whose record_time fallback is launchTime (unix seconds).
func NewTransformer(launchTime int64) *Transformer {
	return &Transformer{launchTime: launchTime}
}

// Transform converts one job ad into its document and unique id. ok is false when the ad must
// be skipped: a DAG root (TaskType == "ROOT"), or one missing JobStatus/GlobalJobId (which
// adstash accesses unguarded, so such ads raise and are dropped).
func (t *Transformer) Transform(ad *classad.ClassAd) (id string, doc map[string]any, ok bool) {
	// Drop DAG root nodes.
	if v := ad.EvaluateAttr("TaskType"); v.IsString() {
		if s, _ := v.StringValue(); s == "ROOT" {
			return "", nil, false
		}
	}
	// record_time accesses ad["JobStatus"] and unique_doc_id reads doc["GlobalJobId"]
	// unguarded; an ad missing either is skipped.
	js, jsOK := ad.EvaluateAttrInt("JobStatus")
	if !jsOK {
		return "", nil, false
	}
	gjid, gjOK := evalString(ad, "GlobalJobId")
	if !gjOK {
		return "", nil, false
	}

	doc = map[string]any{}
	rt := t.recordTime(ad, js)
	doc["RecordTime"] = rt
	doc["ScheddName"] = splitFirst(gjid, "#")

	remoteHost := stringDefault(ad, "RemoteHost", stringDefault(ad, "LastRemoteHost", "UNKNOWN@UNKNOWN"))
	doc["StartdSlot"] = splitFirst(remoteHost, "@")
	doc["StartdName"] = splitLast(remoteHost, "@")

	if name, ok := statusNames[js]; ok {
		doc["Status"] = name
	} else {
		doc["Status"] = "Unknown"
	}
	ju, _ := ad.EvaluateAttrInt("JobUniverse")
	if name, ok := universeNames[ju]; ok {
		doc["Universe"] = name
	} else {
		doc["Universe"] = "Unknown"
	}

	t.bulkConvert(ad, doc)

	// unique_doc_id: "<GlobalJobId>#<RecordTime>".
	id = fmt.Sprintf("%s#%d", gjid, rt)
	return id, doc, true
}

// recordTime ports convert.py record_time: for terminal statuses (Removed/Completed/Error) a
// positive CompletionDate wins, else EpochWriteDate, else EnteredCurrentStatus, else the
// process launch time.
func (t *Transformer) recordTime(ad *classad.ClassAd, jobStatus int64) int64 {
	if jobStatus == 3 || jobStatus == 4 || jobStatus == 6 {
		if cd, ok := ad.EvaluateAttrInt("CompletionDate"); ok && cd > 0 {
			return cd
		}
	}
	if ew, ok := ad.EvaluateAttrInt("EpochWriteDate"); ok && ew > 0 {
		return ew
	}
	if ec, ok := ad.EvaluateAttrInt("EnteredCurrentStatus"); ok && ec > 0 {
		return ec
	}
	return t.launchTime
}

// bulkConvert ports convert.py bulk_convert_ad_data: normalize each attribute name, drop
// ignored attrs, evaluate the expression, and coerce the value by category, using the
// _EXPR/_STRING suffix conventions on failure. Results are written into doc.
func (t *Transformer) bulkConvert(ad *classad.ClassAd, doc map[string]any) {
	for _, raw := range ad.GetAttributes() {
		key := caseNormalize(raw)
		if ignoreSet.has(key) {
			continue // redaction (secrets/env/etc.)
		}
		v := ad.EvaluateAttr(raw)
		if v.IsError() || v.IsUndefined() {
			// Could not evaluate: store the raw expression under "<key>_EXPR".
			if expr, ok := ad.Lookup(raw); ok {
				doc[key+"_EXPR"] = truncate(expr.String())
			}
			continue
		}

		var outKey string
		var outVal any
		switch {
		case indexedKeywordSet.has(key) || noindexKeywordSet.has(key):
			outKey, outVal = key, valueString(v)
		case floatSet.has(key):
			if f, ok := valueFloat(v); ok {
				outKey, outVal = key, f
			} else {
				outKey, outVal = key+"_STRING", valueString(v)
			}
		case intSet.has(key) || strings.HasSuffix(key, "Provisioned") || strings.HasPrefix(key, "Request"):
			if n, ok := valueInt(v); ok {
				outKey, outVal = key, n
			} else {
				outKey, outVal = key+"_STRING", valueString(v)
			}
		case boolSet.has(key) || strings.HasPrefix(key, "Want") || strings.HasPrefix(key, "Has") || strings.HasPrefix(key, "Is"):
			outKey, outVal = key, valueBool(v)
		case dateSet.has(key) || strings.HasSuffix(key, "Date"):
			n, ok := valueInt(v)
			if !ok {
				outKey, outVal = key+"_STRING", valueString(v)
			} else if n == 0 {
				continue // zero-valued dates are dropped entirely
			} else {
				outKey, outVal = key, n
			}
		case nestedSet.has(key):
			if m, ok := valueNested(v); ok {
				outKey, outVal = key, m
			} else {
				outKey, outVal = key+"_STRING", valueString(v)
			}
		default:
			outKey, outVal = key, valueString(v)
		}

		if s, ok := outVal.(string); ok {
			outVal = truncate(s)
		}
		doc[outKey] = outVal
	}
}

// truncate mirrors adstash's 256-char cap: strings longer than 256 become value[:253]+"...".
func truncate(s string) string {
	if len(s) > 256 {
		return s[:253] + "..."
	}
	return s
}

// --- value coercion helpers (classad.Value -> Go/JSON) ---

// valueString renders a Value the way Python str(value) would after ad.eval: unquoted strings,
// plain integers/reals, lowercase bools; anything else via the ClassAd display form.
func valueString(v classad.Value) string {
	switch {
	case v.IsString():
		s, _ := v.StringValue()
		return s
	case v.IsInteger():
		n, _ := v.IntValue()
		return fmt.Sprintf("%d", n)
	case v.IsReal():
		r, _ := v.RealValue()
		return fmt.Sprintf("%g", r)
	case v.IsBool():
		b, _ := v.BoolValue()
		if b {
			return "true"
		}
		return "false"
	default:
		return v.String()
	}
}

// valueFloat coerces to float64 (float() in adstash); ok is false when non-numeric.
func valueFloat(v classad.Value) (float64, bool) {
	if v.IsNumber() {
		f, err := v.NumberValue()
		return f, err == nil
	}
	return 0, false
}

// valueInt coerces to int64 (int() in adstash), truncating reals; ok is false when non-numeric.
func valueInt(v classad.Value) (int64, bool) {
	switch {
	case v.IsInteger():
		n, err := v.IntValue()
		return n, err == nil
	case v.IsNumber():
		f, err := v.NumberValue()
		return int64(f), err == nil
	default:
		return 0, false
	}
}

// valueBool coerces to bool the way Python bool(value) does (never raises): numbers are truthy
// when nonzero, strings when non-empty, bools as-is; other kinds are truthy.
func valueBool(v classad.Value) bool {
	switch {
	case v.IsBool():
		b, _ := v.BoolValue()
		return b
	case v.IsNumber():
		f, _ := v.NumberValue()
		return f != 0
	case v.IsString():
		s, _ := v.StringValue()
		return s != ""
	case v.IsUndefined() || v.IsError():
		return false
	default:
		return true
	}
}

// valueNested coerces a nested ClassAd value to a map; ok is false when it is not a ClassAd.
func valueNested(v classad.Value) (map[string]any, bool) {
	if !v.IsClassAd() {
		return nil, false
	}
	nested, err := v.ClassAdValue()
	if err != nil || nested == nil {
		return nil, false
	}
	m := map[string]any{}
	for _, k := range nested.GetAttributes() {
		m[k] = valueString(nested.EvaluateAttr(k))
	}
	return m, true
}

// --- small string helpers ---

func splitFirst(s, sep string) string {
	if i := strings.Index(s, sep); i >= 0 {
		return s[:i]
	}
	return s
}

func splitLast(s, sep string) string {
	if i := strings.LastIndex(s, sep); i >= 0 {
		return s[i+len(sep):]
	}
	return s
}

func evalString(ad *classad.ClassAd, name string) (string, bool) {
	v := ad.EvaluateAttr(name)
	if !v.IsString() {
		return "", false
	}
	s, err := v.StringValue()
	return s, err == nil
}

func stringDefault(ad *classad.ClassAd, name, def string) string {
	if s, ok := evalString(ad, name); ok {
		return s
	}
	return def
}
