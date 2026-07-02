package app

import "testing"

func TestParseMemMax(t *testing.T) {
	cases := []struct {
		in     string
		want   int64
		wantOK bool
	}{
		{"134217728\n", 134217728, true},   // 128Mi, cgroup v2 numeric
		{"  268435456  ", 268435456, true}, // whitespace trimmed
		{"max", 0, false},                  // v2 unlimited
		{"9223372036854771712", 0, false},  // v1 unlimited sentinel
		{"", 0, false},                     // empty
		{"garbage", 0, false},              // non-numeric
		{"-1", 0, false},                   // negative
		{"0", 0, false},                    // zero
	}
	for _, c := range cases {
		got, ok := parseMemMax(c.in)
		if got != c.want || ok != c.wantOK {
			t.Errorf("parseMemMax(%q) = (%d, %v), want (%d, %v)", c.in, got, ok, c.want, c.wantOK)
		}
	}
}
