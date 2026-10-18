package daemon

import "testing"

func TestRingWrap(t *testing.T) {
	r := NewRing(8)
	r.Write([]byte("abcdef"))
	if got := string(r.Snapshot()); got != "abcdef" {
		t.Fatalf("snapshot = %q", got)
	}
	r.Write([]byte("ghij"))
	if got := string(r.Snapshot()); got != "cdefghij" {
		t.Fatalf("after wrap snapshot = %q", got)
	}
	if got := string(r.Tail(4)); got != "ghij" {
		t.Fatalf("tail = %q", got)
	}
	r.Write([]byte("0123456789"))
	if got := string(r.Snapshot()); got != "23456789" {
		t.Fatalf("after oversized write = %q", got)
	}
}

func TestQuoteShellCommand(t *testing.T) {
	cases := map[string]string{
		"":             "''",
		"echo hi":      "'echo hi'",
		"it's":         `'it'\''s'`,
		"line1\nline2": "'line1\nline2'",
	}
	for in, want := range cases {
		if got := shQuote(in); got != want {
			t.Errorf("shQuote(%q) = %q want %q", in, got, want)
		}
	}
}

func TestSanitizeName(t *testing.T) {
	if got := sanitizeName("main"); got != "main" {
		t.Errorf("got %q", got)
	}
	if got := sanitizeName("a/b c"); got != "a_b_c" {
		t.Errorf("got %q", got)
	}
	if got := sanitizeName(""); got != "main" {
		t.Errorf("got %q", got)
	}
}
