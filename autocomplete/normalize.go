package autocomplete

import (
	"strings"
	"unicode"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

// foldCaser performs Unicode full case folding (language-agnostic).
var foldCaser = cases.Fold()

// Normalize applies the single, documented text pipeline used for both
// matching and identity (deduplication):
//
//  1. Unicode NFKC: compatibility decomposition followed by canonical
//     composition. This unifies full-width/half-width forms ("Ａ" == "A",
//     "，" == ",") and compatibility ligatures, and puts combining marks in a
//     canonical order.
//  2. Unicode full case folding ("ß" -> "ss", "İ" -> "i̇" then NFKC) so that
//     matching is case-insensitive in the Unicode sense.
//  3. NFC: a final canonical composition yields one stable byte sequence per
//     normalized word (canonical vs. combining-mark input converge).
//  4. Trimming of leading/trailing Unicode whitespace and collapsing internal
//     whitespace runs to a single ASCII space.
//
// The original, pre-normalization text is always kept on the entry for
// display; only the normalized form participates in matching and identity.
func Normalize(s string) string {
	s = norm.NFKC.String(s)
	s = foldCaser.String(s)
	s = norm.NFC.String(s)
	return collapseSpace(s)
}

func collapseSpace(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	pending := false
	started := false
	for _, r := range s {
		if unicode.IsSpace(r) {
			pending = true
			continue
		}
		if pending && started {
			b.WriteByte(' ')
		}
		b.WriteRune(r)
		started = true
		pending = false
	}
	return b.String()
}

// RuneLen returns the number of runes in an already-normalized string.
func RuneLen(s string) int {
	return len([]rune(s))
}
