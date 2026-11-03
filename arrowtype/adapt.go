package arrowtype

import (
	"github.com/PelicanPlatform/classad/collections"
)

// Bridging the store's measurement to this package's input.
//
// Measured is a local type rather than the store's profile struct, for two reasons. The
// decision needs only a handful of facts, and stating exactly those keeps the rules readable;
// and it keeps the rule engine free of a storage dependency, so it can be reasoned about and
// tested without one. The cost of that choice is that two shapes could drift apart, and this
// adapter is what pins them: it is the one place the correspondence is written down, so a
// change on either side fails to compile here rather than quietly producing wrong types.
//
// db.AttrTypeProfile is a type ALIAS of collections.AttrTypeProfile, so a profile that arrives
// over dbrpc satisfies this signature with no second adapter.

// FromProfile converts one measured profile into the derivation's input. sampled is the
// profile's sample size, which the store reports once for the whole report rather than per
// attribute -- Present against it is the absence rate, and without it a profile cannot say
// whether a column needs to admit nulls.
//
// ValueClasses and Nulls come from the profile's own accessors rather than from its raw
// Classes map, because undefined and error carry no type: they are nulls, and counting them as
// competing classes is what would send a uniformly-string attribute to a fallback type. On a
// real AP that mistake invents mixed attributes that are not mixed -- one was 25 strings
// against 1975 undefined.
func FromProfile(p collections.AttrTypeProfile, sampled int) Measured {
	return FromProfileComplete(p, sampled, false)
}

// FromProfileComplete is FromProfile told whether the sample covered the whole table. Only a
// complete sample can support a non-nullable column; see Measured.Complete.
//
// EVALUATED CLASSES WIN WHERE THE STORE MEASURED THEM. A profile reports what the cells STORE,
// and an encoder that carries EVALUATED values rather than stored ones puts something
// else in the column -- so typing from the stored classes types the wrong thing. The effect is
// not subtle on real data: MemoryUsage is stored as an expression in 1996 of 2000 history ads
// and evaluates to an integer in all 2000, and reading the stored classes makes `expr` its
// dominant class and leaves it with no column at all.
//
// The evaluated counts are absent from a store too old to measure them, and then the stored
// ones are all there is. That is the OLD answer rather than a degraded one, so a caller that
// needs the evaluated types asks for them explicitly and hears about it (dbrpc's
// ErrEvalTypesUnsupported) rather than inferring their absence from a schema missing columns.
func FromProfileComplete(p collections.AttrTypeProfile, sampled int, complete bool) Measured {
	counts := p.AttrValueCounts
	if p.Evaluated != nil {
		counts = *p.Evaluated
	}
	return Measured{
		Name:         p.Name,
		Sampled:      sampled,
		Present:      p.Present,
		Complete:     complete,
		ValueClasses: counts.ValueClasses(),
		Nulls:        counts.Nulls(),
		LossyAsFloat: counts.IntsLossyAsFloat,
	}
}

// FromProfiles converts a whole report, preserving order. The store reports attributes
// most-present-first, and Schema turns that into field order, so preserving it here is what
// keeps the schema's columns in a useful and stable sequence.
func FromProfiles(profs []collections.AttrTypeProfile, sampled int) []Measured {
	return FromProfilesComplete(profs, sampled, false)
}

// FromProfilesComplete is FromProfiles told whether the sample covered the whole table.
//
// A caller knows this when the profile returned FEWER records than it asked for: the sample
// ran out of records rather than hitting its cap, so it saw everything. That is the only
// sound basis for declaring a column non-nullable.
func FromProfilesComplete(profs []collections.AttrTypeProfile, sampled int, complete bool) []Measured {
	out := make([]Measured, 0, len(profs))
	for _, p := range profs {
		out = append(out, FromProfileComplete(p, sampled, complete))
	}
	return out
}
