package main

import (
	"strings"
	"testing"
)

// The usage text is the only description of the language a user gets without starting the
// shell, so it is held to the same standard as the shell's own .help: it names what the
// language has, and never denies something the parser accepts. A claim that the language
// cannot join outlives the limitation it described, and sends people to write two queries
// and pair the rows by hand.
func TestUsageTextNamesTheJoinForms(t *testing.T) {
	for _, want := range []string{"INNER/LEFT/CROSS", "JOIN", "UNION"} {
		if !strings.Contains(usageText, want) {
			t.Errorf("usage text does not name %q; the language supports it", want)
		}
	}
}

func TestUsageTextDoesNotDenyJoins(t *testing.T) {
	// Phrases that have appeared here and in the REPL's help, each of which was true of
	// an earlier store and is not true of this one.
	for _, deny := range []string{
		"no joins",
		"single ClassAd collection",
		"JOINs are not supported",
		"are not supported",
	} {
		if strings.Contains(usageText, deny) {
			t.Errorf("usage text claims %q, which the parser contradicts", deny)
		}
	}
}
