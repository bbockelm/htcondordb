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
func FromProfileComplete(p collections.AttrTypeProfile, sampled int, complete bool) Measured {
	return Measured{
		Name:         p.Name,
		Sampled:      sampled,
		Present:      p.Present,
		Complete:     complete,
		ValueClasses: p.ValueClasses(),
		Nulls:        p.Nulls(),
		LossyAsFloat: p.IntsLossyAsFloat,
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
