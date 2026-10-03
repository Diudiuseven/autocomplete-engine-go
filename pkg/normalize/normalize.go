// Package normalize defines the single canonical normalization rule used for
// all matching: Unicode NFC composition, full-width to half-width folding,
// and case folding. The original display form is always preserved separately.
package normalize

import (
	"unicode/utf8"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
	"golang.org/x/text/width"
)

// fold is the full case folder (e.g. "ß" -> "ss").
var fold = cases.Fold()

// Key returns the canonical matching key for s.
//
// The rule is identical for indexing and querying:
//  1. NFC composition (combining marks compose with base runes),
//  2. width folding (full-width ASCII / punctuation -> half-width),
//  3. full case folding (case-insensitive matching).
func Key(s string) string {
	s = norm.NFC.String(s)
	s = width.Fold.String(s)
	s = fold.String(s)
	return s
}

// Valid reports whether s is acceptable as an index term or query:
// valid UTF-8, non-empty, no control characters.
func Valid(s string) bool {
	if s == "" || !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

// RuneCount returns the number of runes (for length limits).
func RuneCount(s string) int { return utf8.RuneCountInString(s) }
