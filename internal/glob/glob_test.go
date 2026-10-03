package glob

import (
	"strings"
	"testing"
)

func TestMatch(t *testing.T) {
	cases := []struct {
		pat, s string
		want   bool
	}{
		{"*", "", true}, {"*", "abc", true}, {"a*", "abc", true}, {"*c", "abc", true},
		{"a*c", "abc", true}, {"a*c", "ac", true}, {"a*c", "ab", false},
		{"a?c", "abc", true}, {"a?c", "ac", false}, {"", "", true}, {"", "a", false},
		{"h[ae]llo", "hello", true}, {"h[ae]llo", "hallo", true}, {"h[ae]llo", "hillo", false},
		{"h[^e]llo", "hallo", true}, {"h[^e]llo", "hello", false},
		{"h[a-c]llo", "hbllo", true}, {"h[a-c]llo", "hdllo", false},
		{"h[c-a]llo", "hbllo", true},
		{`a\*b`, "a*b", true}, {`a\*b`, "axb", false}, {`a\`, `a\`, true},
		{"**a**", "xay", true}, {"*a*b*c*", "xxaxxbxxcxx", true}, {"*a*b*c*", "xxaxxcxxbxx", false},
		{`[\]]`, "]", true}, {"[abc", "a", true}, {"user:*", "user:42", true}, {"user:*", "usr:42", false},
		{"a*", "", false}, {"?", "", false}, {"news.*", "news.sports", true}, {"news.*", "weather", false},
	}
	for _, c := range cases {
		if got := Match(c.pat, c.s); got != c.want {
			t.Errorf("Match(%q, %q) = %v, want %v", c.pat, c.s, got, c.want)
		}
	}
	// A pathological pattern must finish quickly (no exponential backtracking).
	if Match(strings.Repeat("a*", 30)+"b", strings.Repeat("a", 5000)) {
		t.Error("should not match")
	}
}

func FuzzMatch(f *testing.F) {
	f.Add("a*b", "aXb")
	f.Add("[a-", "a")
	f.Add(`\`, `\`)
	f.Fuzz(func(t *testing.T, pat, s string) { _ = Match(pat, s) })
}
