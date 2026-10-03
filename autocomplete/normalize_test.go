package autocomplete

import "testing"

func TestNormalize(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"lowercase", "Hello", "hello"},
		{"fullwidth latin", "ＨＥＬＬＯ", "hello"},
		{"halfwidth katakana->fullwidth NFKC", "ﾊﾛｰ", "ハロー"},
		{"german sharp s full fold", "Straße", "strasse"},
		{"greek capital keeps tonos", "ΌΣΑ", "όσα"},
		{"combining accent equals precomposed", "café", "café"},
		{"composed equals decomposed", "é", "é"}, // é (U+00E9) vs e + U+0301
		{"whitespace collapse", "  foo   bar\tbaz \n", "foo bar baz"},
		{"ligature fi", "ﬁle", "file"},
		{"turkish dotted capital i folded", "İ", "i̇"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Normalize(c.in); got != c.want {
				t.Errorf("Normalize(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
	// composed vs decomposed explicitly
	if Normalize("é") != Normalize("é") {
		t.Fatal("precomposed and combining sequences must normalize equally")
	}
}

func TestRuneLen(t *testing.T) {
	if RuneLen("héllo") != 5 {
		t.Fatalf("rune len mismatch")
	}
}
