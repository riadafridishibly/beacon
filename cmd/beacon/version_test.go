package main

import (
	"runtime/debug"
	"testing"
)

func TestVersion(t *testing.T) {
	t.Parallel()
	vcs := func(modified string) []debug.BuildSetting {
		return []debug.BuildSetting{
			{Key: "vcs.revision", Value: "3d5ce30abcdef0123456789abcdef0123456789a"},
			{Key: "vcs.modified", Value: modified},
		}
	}
	for _, tc := range []struct {
		main     string
		settings []debug.BuildSetting
		want     string
	}{
		{"v1.2.0", vcs("false"), "v1.2.0"},
		{"v1.2.0+dirty", vcs("true"), "v1.2.0+dirty"},
		{"v0.0.0-20261111093000-3d5ce30abcde", vcs("false"), "3d5ce30"},
		{"v0.0.0-20261111093000-3d5ce30abcde+dirty", vcs("true"), "3d5ce30+dirty"},
		{"(devel)", nil, "devel"},
		{"", nil, "devel"},
	} {
		bi := &debug.BuildInfo{Main: debug.Module{Version: tc.main}, Settings: tc.settings}
		if got := version(bi); got != tc.want {
			t.Errorf("%q: %q, want %q", tc.main, got, tc.want)
		}
	}
}
