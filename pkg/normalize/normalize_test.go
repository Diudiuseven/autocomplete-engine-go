package normalize

import "testing"

func TestKeyCaseFold(t *testing.T) {
	cases := [][2]string{
		{"Hello", "hello"},
		{"HELLO", "hello"},
		{"Straße", "strasse"}, // full case folding
	}
	for _, c := range cases {
		if got := Key(c[0]); got != c[1] {
			t.Errorf("Key(%q) = %q, want %q", c[0], got, c[1])
		}
	}
}

func TestKeyWidthFold(t *testing.T) {
	// full-width ASCII -> half-width, then case fold
	if got := Key("Ａｐｐｌｅ"); got != "apple" {
		t.Errorf("Key(full-width) = %q, want apple", got)
	}
}

func TestKeyNFC(t *testing.T) {
	// e + combining acute -> composed é
	decomposed := "é"
	composed := "é"
	if got := Key(decomposed); got != Key(composed) {
		t.Errorf("NFC mismatch: %q vs %q", got, Key(composed))
	}
}

func TestKeyIdempotent(t *testing.T) {
	s := "Ｈｅｌｌｏ World"
	if Key(Key(s)) != Key(s) {
		t.Error("Key is not idempotent")
	}
}

func TestValid(t *testing.T) {
	if !Valid("hello") {
		t.Error("plain string should be valid")
	}
	if Valid("") {
		t.Error("empty should be invalid")
	}
	if Valid("bad\x00char") {
		t.Error("control char should be invalid")
	}
	if Valid("bad\x7fchar") {
		t.Error("DEL should be invalid")
	}
}
